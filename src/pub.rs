use crate::endpoint::Endpoint;
use crate::error::ZmqResult;
use crate::message::*;
use crate::runtime::AcceptStopHandle;
use crate::util::PeerIdentity;
use crate::{codec::*, ZmqError};
use crate::{runtime as async_rt, CaptureSocket, SocketOptions};
use crate::{MultiPeerBackend, Socket, SocketBackend, SocketEvent, SocketSend, SocketType};

use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::{future, select, FutureExt, SinkExt, StreamExt};
use parking_lot::Mutex;

use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

enum ConnectionCommand {
    New(PeerIdentity, ZmqFramedWrite, oneshot::Sender<()>),
    Closed(PeerIdentity),
}

enum SubscriptionCommand {
    Subscribe(PeerIdentity, Vec<u8>),
    Unsubscribe(PeerIdentity, Vec<u8>),
}

pub(crate) struct Subscriber {
    pub(crate) subscriptions: Vec<Vec<u8>>,
    pub(crate) send_queue: ZmqFramedWrite,
    _subscription_coro_stop: oneshot::Sender<()>,
}

pub(crate) struct PubSocketBackend {
    connections_command: std::sync::mpsc::Sender<ConnectionCommand>,
    subscriptions_command: std::sync::mpsc::Sender<SubscriptionCommand>,
    socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    socket_options: SocketOptions,
}

impl PubSocketBackend {
    fn message_received(&self, peer_id: &PeerIdentity, message: Message) {
        let data = match message {
            Message::Message(m) => {
                if m.len() != 1 {
                    log::warn!("Received message with unexpected length: {}", m.len());
                    return;
                }
                m.into_vec().pop().unwrap_or_default()
            }
            _ => return,
        };

        if data.is_empty() {
            return;
        }

        match data.first() {
            Some(1) => {
                let _ = self
                    .subscriptions_command
                    .send(SubscriptionCommand::Subscribe(
                        peer_id.clone(),
                        Vec::from(&data[1..]),
                    ));
            }
            Some(0) => {
                let _ = self
                    .subscriptions_command
                    .send(SubscriptionCommand::Unsubscribe(
                        peer_id.clone(),
                        Vec::from(&data[1..]),
                    ));
            }
            _ => log::warn!(
                "Received message with unexpected first byte: {:?}",
                data.first()
            ),
        }
    }
}

impl SocketBackend for PubSocketBackend {
    fn socket_type(&self) -> SocketType {
        SocketType::PUB
    }

    fn socket_options(&self) -> &SocketOptions {
        &self.socket_options
    }

    fn shutdown(&self) {}

    fn monitor(&self) -> &Mutex<Option<mpsc::Sender<SocketEvent>>> {
        &self.socket_monitor
    }
}

#[async_trait]
impl MultiPeerBackend for PubSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (mut recv_queue, send_queue) = io.into_parts();
        let (sender, stop_receiver) = oneshot::channel();
        let _ = self.connections_command.send(ConnectionCommand::New(
            peer_id.to_owned(),
            send_queue,
            sender,
        ));

        let backend = self;
        let peer_id = peer_id.clone();
        async_rt::task::spawn(async move {
            let mut stop_receiver = stop_receiver.fuse();
            loop {
                select! {
                     _ = stop_receiver => {
                         break;
                     },
                     message = recv_queue.next().fuse() => {
                        match message {
                            Some(Ok(m)) => backend.message_received(&peer_id, m),
                            Some(Err(e)) => {
                                log::debug!("Error receiving message: {:?}", e);
                                backend.peer_disconnected(&peer_id);
                                break;
                            }
                            None => {
                                backend.peer_disconnected(&peer_id);
                                break
                            }
                        }

                     }
                }
            }
        });
    }

    fn peer_disconnected(&self, peer_id: &PeerIdentity) {
        let _ = self
            .connections_command
            .send(ConnectionCommand::Closed(peer_id.to_owned()));
    }
}

pub struct PubSocket {
    pub(crate) backend: Arc<PubSocketBackend>,
    subscribers: HashMap<PeerIdentity, Subscriber>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
    connections_commands: std::sync::mpsc::Receiver<ConnectionCommand>,
    subscription_commands: std::sync::mpsc::Receiver<SubscriptionCommand>,
}

impl PubSocket {
    fn disconnect_peer(&mut self, peer_id: PeerIdentity) {
        log::info!("Client disconnected {:?}", peer_id);
        if let Some(monitor) = self.backend.monitor().lock().as_mut() {
            // TODO simplify me
            let _ = monitor.try_send(SocketEvent::Disconnected(peer_id.clone()));
        }
        self.subscribers.remove(&peer_id);
    }

    fn process_connections(&mut self) -> Result<(), ZmqError> {
        use std::sync::mpsc::TryRecvError;
        loop {
            let command_message = self.connections_commands.try_recv();
            match command_message {
                Ok(ConnectionCommand::New(peer_id, send_queue, sender)) => {
                    self.subscribers.insert(
                        peer_id.clone(),
                        Subscriber {
                            subscriptions: vec![],
                            send_queue: send_queue,
                            _subscription_coro_stop: sender,
                        },
                    );
                }
                Ok(ConnectionCommand::Closed(peer_id)) => self.disconnect_peer(peer_id),
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    return Err(ZmqError::Other("Command channel closed"))
                }
            }
        }
    }

    fn process_subscriptions(&mut self) -> Result<(), ZmqError> {
        use std::sync::mpsc::TryRecvError;
        loop {
            let subscription_command = self.subscription_commands.try_recv();
            match subscription_command {
                Ok(SubscriptionCommand::Subscribe(peer_id, data)) => {
                    if let Some(entry) = self.subscribers.get_mut(&peer_id) {
                        entry.subscriptions.push(data);
                    }
                }
                Ok(SubscriptionCommand::Unsubscribe(peer_id, data)) => {
                    if let Some(entry) = self.subscribers.get_mut(&peer_id) {
                        if let Some(index) = entry.subscriptions.iter().position(|s| s == &data) {
                            entry.subscriptions.remove(index);
                        }
                    }
                }
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    return Err(ZmqError::Other("Command channel closed"))
                }
            }
        }
    }
}

impl Drop for PubSocket {
    fn drop(&mut self) {
        self.backend.shutdown();
    }
}

#[async_trait]
impl SocketSend for PubSocket {
    async fn send(&mut self, message: ZmqMessage) -> ZmqResult<()> {
        self.process_connections()?;
        self.process_subscriptions()?;

        let first_frame = match message.get(0) {
            Some(frame) => frame.clone(),
            None => return Ok(()), // Empty message, nothing to publish
        };

        let msg_envelope = Message::Message(message);
        let fanout = self
            .subscribers
            .iter_mut()
            .filter(|(_id, subscriber)| {
                subscriber.subscriptions.iter().any(|sub_filter| {
                    sub_filter.len() <= first_frame.len()
                        && sub_filter.as_slice() == &first_frame[0..sub_filter.len()]
                })
            })
            .map(|(id, subscriber)| async {
                (id.clone(), subscriber.send_queue.send(&msg_envelope).await)
            })
            .collect::<Vec<_>>();

        let results = future::join_all(fanout).await;

        let mut final_result = Ok(());
        for (peer, result) in results {
            match result {
                Ok(()) => {}
                Err(CodecError::Io(e)) => {
                    if e.kind() == ErrorKind::BrokenPipe {
                        self.disconnect_peer(peer);
                    } else {
                        log::error!("Error sending message: {:?}", e);
                    }
                }
                Err(e) => {
                    log::error!("Error sending message: {:?}", e);
                    final_result = Err(e.into());
                }
            }
        }
        final_result
    }
}

impl CaptureSocket for PubSocket {}

#[async_trait]
impl Socket for PubSocket {
    fn with_options(options: SocketOptions) -> Self {
        let (conn_tx, conn_rx) = std::sync::mpsc::channel();
        let (subs_tx, subs_rx) = std::sync::mpsc::channel();
        Self {
            subscribers: HashMap::new(),
            connections_commands: conn_rx,
            subscription_commands: subs_rx,
            backend: Arc::new(PubSocketBackend {
                connections_command: conn_tx,
                subscriptions_command: subs_tx,
                socket_monitor: Mutex::new(None),
                socket_options: options,
            }),
            binds: HashMap::new(),
        }
    }

    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle> {
        &mut self.binds
    }

    fn monitor(&mut self) -> mpsc::Receiver<SocketEvent> {
        let (sender, receiver) = mpsc::channel(1024);
        self.backend.socket_monitor.lock().replace(sender);
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::tests::{
        test_bind_to_any_port_helper, test_bind_to_unspecified_interface_helper,
    };
    use crate::ZmqResult;
    use std::net::IpAddr;

    #[async_rt::test]
    async fn test_bind_to_any_port() -> ZmqResult<()> {
        let s = PubSocket::new();
        test_bind_to_any_port_helper(s).await
    }

    #[async_rt::test]
    async fn test_bind_to_any_ipv4_interface() -> ZmqResult<()> {
        let any_ipv4: IpAddr = "0.0.0.0".parse().unwrap();
        let s = PubSocket::new();
        test_bind_to_unspecified_interface_helper(any_ipv4, s, 4000).await
    }

    #[async_rt::test]
    async fn test_bind_to_any_ipv6_interface() -> ZmqResult<()> {
        let any_ipv6: IpAddr = "::".parse().unwrap();
        let s = PubSocket::new();
        test_bind_to_unspecified_interface_helper(any_ipv6, s, 4010).await
    }
}
