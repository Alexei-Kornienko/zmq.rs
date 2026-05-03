pub use ::monoio::{main, test};

use crate::runtime::FramedIo;

fn make_framed<T>(stream: T) -> FramedIo
where
    T: ::monoio::io::AsyncReadRent
        + ::monoio::io::AsyncWriteRent
        + ::monoio::io::Splitable
        + 'static,
{
    let (read, write) = stream.into_split();
    FramedIo::new(Box::new(read), Box::new(write))
}

pub mod task {
    use std::any::Any;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[track_caller]
    pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
    where
        T: Future + 'static,
        T::Output: 'static,
    {
        ::monoio::spawn(task).into()
    }

    /// The type of error that occurred in the task. See [`JoinHandle`].
    ///
    /// monoio task failures are not currently normalized beyond cancellation.
    #[derive(Debug)]
    pub enum JoinError {
        Cancelled,
        Panic(Box<dyn Any + Send + 'static>),
    }

    impl JoinError {
        pub fn is_cancelled(&self) -> bool {
            matches!(self, Self::Cancelled)
        }

        pub fn is_panic(&self) -> bool {
            !self.is_cancelled()
        }
    }

    pub struct JoinHandle<T>(::monoio::task::JoinHandle<T>);

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            ::monoio::task::JoinHandle::poll(Pin::new(&mut self.0), cx).map(Ok)
        }
    }

    impl<T> From<::monoio::task::JoinHandle<T>> for JoinHandle<T> {
        fn from(h: ::monoio::task::JoinHandle<T>) -> Self {
            Self(h)
        }
    }

    pub async fn sleep(duration: std::time::Duration) {
        ::monoio::time::sleep(duration).await;
    }

    pub async fn timeout<F, T>(
        duration: std::time::Duration,
        f: F,
    ) -> std::result::Result<T, Box<dyn std::error::Error>>
    where
        F: Future<Output = T>,
    {
        Ok(::monoio::time::timeout(duration, f).await?)
    }
}

#[cfg(feature = "tcp-transport")]
pub(crate) mod tcp {
    use super::make_framed;
    use crate::endpoint::{Endpoint, Host, Port};
    use crate::runtime::FramedIo;
    use crate::runtime::{task, AcceptStopHandle};
    use crate::task_handle::TaskHandle;
    use crate::ZmqResult;

    use ::monoio::net::tcp::{TcpListener, TcpStream};
    use futures::{select, FutureExt};

    pub(crate) async fn connect(host: &Host, port: Port) -> ZmqResult<(FramedIo, Endpoint)> {
        let raw_socket = TcpStream::connect((host.to_string().as_str(), port)).await?;
        #[cfg(not(windows))]
        raw_socket.set_nodelay(true)?;
        let peer_addr = raw_socket.peer_addr()?;

        Ok((make_framed(raw_socket), Endpoint::from_tcp_addr(peer_addr)))
    }

    pub(crate) async fn begin_accept<T>(
        mut host: Host,
        port: Port,
        cback: impl Fn(ZmqResult<(FramedIo, Endpoint)>) -> T + Send + 'static,
    ) -> ZmqResult<(Endpoint, AcceptStopHandle)>
    where
        T: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind((host.to_string().as_str(), port))?;
        let resolved_addr = listener.local_addr()?;
        let (stop_channel, stop_callback) = futures::channel::oneshot::channel::<()>();
        let task_handle = task::spawn(async move {
            let mut stop_callback = stop_callback.fuse();
            loop {
                select! {
                    incoming = listener.accept().fuse() => {
                        let maybe_accepted: Result<_, _> = incoming
                            .and_then(|(raw_socket, remote_addr)| {
                                raw_socket
                                    .set_nodelay(true)
                                    .map(|_| (raw_socket, remote_addr))
                            })
                            .map(|(raw_socket, remote_addr)| {
                                (
                                    make_framed(raw_socket),
                                    Endpoint::from_tcp_addr(remote_addr),
                                )
                            })
                            .map_err(|err| err.into());
                        task::spawn(cback(maybe_accepted));
                    }
                    _ = stop_callback => {
                        break
                    }
                }
            }
            Ok(())
        });
        debug_assert_ne!(resolved_addr.port(), 0);
        let port = resolved_addr.port();
        let resolved_host: Host = resolved_addr.ip().into();
        if let Host::Ipv4(ip) = host {
            debug_assert_eq!(ip, resolved_addr.ip());
            host = resolved_host;
        } else if let Host::Ipv6(ip) = host {
            debug_assert_eq!(ip, resolved_addr.ip());
            host = resolved_host;
        }
        Ok((
            Endpoint::Tcp(host, port),
            AcceptStopHandle(TaskHandle::new(stop_channel, task_handle)),
        ))
    }
}

#[cfg(all(feature = "ipc-transport", target_family = "unix"))]
pub(crate) mod ipc {
    use super::make_framed;
    use crate::endpoint::Endpoint;
    use crate::runtime::FramedIo;
    use crate::runtime::{task, AcceptStopHandle};
    use crate::task_handle::TaskHandle;
    use crate::ZmqResult;

    use ::monoio::net::unix::{SocketAddr, UnixListener, UnixStream};
    use futures::channel::oneshot;
    use futures::{select, FutureExt};
    use std::path::Path;

    fn pathname_from_unix_addr(addr: SocketAddr) -> Option<std::path::PathBuf> {
        addr.as_pathname().map(|a| a.to_owned())
    }

    pub(crate) async fn connect(path: &Path) -> ZmqResult<(FramedIo, Endpoint)> {
        let raw_socket = UnixStream::connect(path).await?;
        let peer_addr = pathname_from_unix_addr(raw_socket.peer_addr()?);

        Ok((make_framed(raw_socket), Endpoint::Ipc(peer_addr)))
    }

    pub(crate) async fn begin_accept<T>(
        path: &Path,
        cback: impl Fn(ZmqResult<(FramedIo, Endpoint)>) -> T + Send + 'static,
    ) -> ZmqResult<(Endpoint, AcceptStopHandle)>
    where
        T: std::future::Future<Output = ()> + Send + 'static,
    {
        let wildcard: &Path = "*".as_ref();
        if path == wildcard {
            todo!("Need to implement support for wildcard paths!");
        }

        let listener = UnixListener::bind(path)?;
        let resolved_addr = Some(path.to_owned());
        let listener_addr = resolved_addr.clone();

        let (stop_channel, stop_callback) = oneshot::channel::<()>();
        let task_handle = task::spawn(async move {
            let mut stop_callback = stop_callback.fuse();
            loop {
                select! {
                    incoming = listener.accept().fuse() => {
                        let maybe_accepted: Result<_, _> = incoming.map(|(raw_socket, peer_addr)| {
                            let peer_addr = pathname_from_unix_addr(peer_addr);
                            (make_framed(raw_socket), Endpoint::Ipc(peer_addr))
                        }).map_err(|err| err.into());
                        task::spawn(cback(maybe_accepted));
                    },
                    _ = stop_callback => {
                        log::debug!("Accept task received stop signal. {:?}", listener_addr);
                        break
                    }
                }
            }
            drop(listener);
            Ok(())
        });
        Ok((
            Endpoint::Ipc(resolved_addr),
            AcceptStopHandle(TaskHandle::new(stop_channel, task_handle)),
        ))
    }
}
