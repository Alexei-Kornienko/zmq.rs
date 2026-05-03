use std::future::Future;

#[cfg(feature = "tokio-runtime")]
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "monoio-runtime")]
type Runtime = monoio::FusionRuntime<
    monoio::time::TimeDriver<monoio::IoUringDriver>,
    monoio::time::TimeDriver<monoio::LegacyDriver>,
>;

pub struct BenchRuntime {
    #[cfg(feature = "tokio-runtime")]
    inner: Runtime,
    #[cfg(feature = "monoio-runtime")]
    inner: std::cell::RefCell<Runtime>,
}

impl BenchRuntime {
    pub fn new() -> Self {
        #[cfg(feature = "tokio-runtime")]
        {
            Self {
                inner: Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("tokio runtime"),
            }
        }

        #[cfg(feature = "async-std-runtime")]
        {
            Self {}
        }

        #[cfg(feature = "async-dispatcher-runtime")]
        {
            async_dispatcher::set_dispatcher(async_dispatcher::thread_dispatcher());
            Self {}
        }

        #[cfg(feature = "monoio-runtime")]
        {
            Self {
                inner: std::cell::RefCell::new(
                    monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_timer()
                        .build()
                        .expect("monoio runtime"),
                ),
            }
        }
    }

    #[allow(clippy::unused_self)]
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        #[cfg(feature = "tokio-runtime")]
        {
            self.inner.block_on(future)
        }

        #[cfg(feature = "async-std-runtime")]
        {
            async_std::task::block_on(future)
        }

        #[cfg(feature = "async-dispatcher-runtime")]
        {
            async_dispatcher::block_on(future)
        }

        #[cfg(feature = "monoio-runtime")]
        {
            self.inner.borrow_mut().block_on(future)
        }
    }
}

impl Default for BenchRuntime {
    fn default() -> Self {
        Self::new()
    }
}
