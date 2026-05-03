#[cfg(not(any(
    feature = "tokio-runtime",
    feature = "async-std-runtime",
    feature = "async-dispatcher-runtime",
    feature = "monoio-runtime"
)))]
compile_error!(
    "exactly one runtime feature must be enabled: tokio-runtime, async-std-runtime, async-dispatcher-runtime, or monoio-runtime"
);

#[cfg(any(
    all(feature = "tokio-runtime", feature = "async-std-runtime"),
    all(feature = "tokio-runtime", feature = "async-dispatcher-runtime"),
    all(feature = "tokio-runtime", feature = "monoio-runtime"),
    all(feature = "async-std-runtime", feature = "async-dispatcher-runtime"),
    all(feature = "async-std-runtime", feature = "monoio-runtime"),
    all(feature = "async-dispatcher-runtime", feature = "monoio-runtime")
))]
compile_error!(
    "only one runtime feature can be enabled at a time: tokio-runtime, async-std-runtime, async-dispatcher-runtime, or monoio-runtime"
);

#[cfg(feature = "async-dispatcher-runtime")]
mod async_dispatcher;
#[cfg(feature = "async-std-runtime")]
mod async_std;
#[cfg(not(feature = "monoio-runtime"))]
mod framed;
#[cfg(feature = "monoio-runtime")]
mod framed_monoio;
#[cfg(feature = "monoio-runtime")]
mod monoio;
#[cfg(feature = "tokio-runtime")]
mod tokio;

#[cfg(feature = "async-dispatcher-runtime")]
use self::async_dispatcher as active;
#[cfg(feature = "async-std-runtime")]
use self::async_std as active;
#[cfg(feature = "monoio-runtime")]
use self::monoio as active;
#[cfg(feature = "tokio-runtime")]
use self::tokio as active;

#[cfg(feature = "async-dispatcher-runtime")]
pub use self::async_dispatcher::task;
#[cfg(all(
    feature = "async-dispatcher-runtime",
    feature = "async-dispatcher-macros"
))]
pub use self::async_dispatcher::{main, test};
#[cfg(feature = "async-std-runtime")]
pub use self::async_std::{main, task, test};
#[cfg(feature = "monoio-runtime")]
pub use self::monoio::{main, task, test};
#[cfg(feature = "tokio-runtime")]
pub use self::tokio::{main, task, test};

#[cfg(not(feature = "monoio-runtime"))]
pub(crate) use self::framed::{FramedIo, ZmqFramedRead, ZmqFramedWrite};
#[cfg(feature = "monoio-runtime")]
pub(crate) use self::framed_monoio::{FramedIo, ZmqFramedRead, ZmqFramedWrite};

use crate::endpoint::Endpoint;
use crate::task_handle::TaskHandle;
use crate::ZmqResult;

pub struct AcceptStopHandle(pub(crate) TaskHandle<()>);

/// Connects to the given endpoint.
///
/// # Panics
/// Panics if the requested endpoint uses a transport type that isn't enabled.
pub(crate) async fn connect(endpoint: &Endpoint) -> ZmqResult<(FramedIo, Endpoint)> {
    match endpoint {
        Endpoint::Tcp(host, port) => {
            #[cfg(feature = "tcp-transport")]
            {
                active::tcp::connect(host, *port).await
            }
            #[cfg(not(feature = "tcp-transport"))]
            {
                let _ = (host, port);
                panic!("feature \"tcp-transport\" is not enabled")
            }
        }
        Endpoint::Ipc(path) => {
            #[cfg(all(feature = "ipc-transport", any(target_family = "unix", windows)))]
            {
                if let Some(path) = path {
                    active::ipc::connect(path).await
                } else {
                    Err(crate::error::ZmqError::Socket(
                        "Cannot connect to an unnamed ipc socket",
                    ))
                }
            }
            #[cfg(not(all(feature = "ipc-transport", any(target_family = "unix", windows))))]
            {
                let _ = path;
                panic!("IPC transport is not available on this platform")
            }
        }
    }
}

/// Spawns an async task that listens for connections at the provided endpoint.
///
/// `cback` will be invoked when a connection is accepted. If the result was
/// `Ok`, it will receive a tuple containing the framed raw socket and the
/// endpoint of the accepted remote connection.
///
/// # Panics
/// Panics if the requested endpoint uses a transport type that isn't enabled.
pub(crate) async fn begin_accept<T>(
    endpoint: Endpoint,
    cback: impl Fn(ZmqResult<(FramedIo, Endpoint)>) -> T + Send + 'static,
) -> ZmqResult<(Endpoint, AcceptStopHandle)>
where
    T: std::future::Future<Output = ()> + Send + 'static,
{
    match endpoint {
        Endpoint::Tcp(host, port) => {
            #[cfg(feature = "tcp-transport")]
            {
                active::tcp::begin_accept(host, port, cback).await
            }
            #[cfg(not(feature = "tcp-transport"))]
            {
                let _ = (host, port, cback);
                panic!("feature \"tcp-transport\" is not enabled")
            }
        }
        Endpoint::Ipc(path) => {
            #[cfg(all(feature = "ipc-transport", any(target_family = "unix", windows)))]
            {
                if let Some(path) = path {
                    active::ipc::begin_accept(&path, cback).await
                } else {
                    Err(crate::error::ZmqError::Socket(
                        "Cannot begin accepting peers at an unnamed ipc socket",
                    ))
                }
            }
            #[cfg(not(all(feature = "ipc-transport", any(target_family = "unix", windows))))]
            {
                let _ = (path, cback);
                panic!("IPC transport is not available on this platform")
            }
        }
    }
}
