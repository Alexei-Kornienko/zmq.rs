#[cfg(test)]
mod test {
    use zeromq::__async_rt as async_rt;
    use zeromq::prelude::*;
    use zeromq::Endpoint;
    use zeromq::ZmqMessage;

    use futures::channel::{mpsc, oneshot};
    use futures::{SinkExt, StreamExt};
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn ipv6_loopback_available() -> bool {
        std::net::TcpListener::bind("[::1]:0").is_ok()
    }

    fn unique_ipc_endpoint(name: &str) -> (String, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "z-pub-sub-{name}-{}-{nanos}.sock",
            std::process::id()
        ));
        (format!("ipc://{}", path.display()), path)
    }

    #[cfg_attr(
        feature = "monoio-runtime",
        async_rt::test(driver = "uring", enable_timer = true)
    )]
    #[cfg_attr(not(feature = "monoio-runtime"), async_rt::test)]
    async fn test_pub_sub_sockets() {
        pretty_env_logger::try_init().ok();

        async fn helper(bind_addr: String) {
            // We will join on these at the end to determine if any tasks we spawned
            // panicked
            let mut task_handles = Vec::new();
            let payload = chrono::Utc::now().to_rfc2822();

            let cloned_payload = payload.clone();
            let (server_stop_sender, mut server_stop) = oneshot::channel::<()>();
            let (has_bound_sender, has_bound) = oneshot::channel::<Endpoint>();
            let bind_addr_for_bind = bind_addr.clone();
            task_handles.push(async_rt::task::spawn(async move {
                let mut pub_socket = zeromq::PubSocket::new();
                let bound_to = pub_socket
                    .bind(&bind_addr_for_bind)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to bind to {}: {}", bind_addr_for_bind, e));
                has_bound_sender
                    .send(bound_to)
                    .expect("channel was dropped");

                loop {
                    if let Ok(Some(_)) = server_stop.try_recv() {
                        break;
                    }

                    let s: String = cloned_payload.clone();
                    let m = ZmqMessage::from(s);
                    pub_socket.send(m).await.expect("Failed to send");
                    async_rt::task::sleep(Duration::from_millis(1)).await;
                }

                let errs = pub_socket.close().await;
                if !errs.is_empty() {
                    panic!("Could not unbind socket: {:?}", errs);
                }
            }));
            // Block until the pub has finished binding
            // TODO: ZMQ sockets should not care about this sort of ordering.
            // See https://github.com/zeromq/zmq.rs/issues/73
            let bound_addr = has_bound.await.expect("channel was cancelled");
            if let Endpoint::Tcp(_host, port) = bound_addr.clone() {
                assert_ne!(port, 0);
            }

            let (sub_results_sender, sub_results) = mpsc::channel(100);
            for _ in 0..10 {
                let mut cloned_sub_sender = sub_results_sender.clone();
                let cloned_payload = payload.clone();
                let cloned_bound_addr = bound_addr.to_string();
                task_handles.push(async_rt::task::spawn(async move {
                    let mut sub_socket = zeromq::SubSocket::new();
                    sub_socket
                        .connect(&cloned_bound_addr)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to connect to {}", cloned_bound_addr));

                    sub_socket.subscribe("").await.expect("Failed to subscribe");

                    async_rt::task::sleep(std::time::Duration::from_millis(500)).await;

                    for _ in 0..10 {
                        let recv_message = sub_socket.recv().await.unwrap();
                        let recv_payload =
                            String::from_utf8(recv_message.get(0).unwrap().to_vec()).unwrap();
                        assert_eq!(cloned_payload, recv_payload);
                        cloned_sub_sender.send(()).await.unwrap();
                    }
                }));
            }
            drop(sub_results_sender);
            let res_vec: Vec<()> = sub_results.collect().await;
            assert_eq!(100, res_vec.len());

            server_stop_sender.send(()).unwrap();
            for t in task_handles {
                t.await.expect("Task failed unexpectedly!");
            }
        }

        let (ipc_a, ipc_a_path) = unique_ipc_endpoint("a");
        let (ipc_b, ipc_b_path) = unique_ipc_endpoint("b");
        let mut addrs = vec![
            "tcp://localhost:0".to_string(),
            "tcp://127.0.0.1:0".to_string(),
            "tcp://127.0.0.1:0".to_string(),
            "tcp://localhost:0".to_string(),
            "tcp://127.0.0.1:0".to_string(),
            ipc_a,
            ipc_b,
        ];
        if ipv6_loopback_available() {
            addrs.push("tcp://[::1]:0".to_string());
            addrs.push("tcp://[::1]:0".to_string());
        }
        futures::future::join_all(addrs.into_iter().map(helper)).await;
        let _ = std::fs::remove_file(ipc_a_path);
        let _ = std::fs::remove_file(ipc_b_path);
    }
}
