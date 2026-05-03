use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use asynchronous_codec::{Decoder, Encoder};
use bytes::{Bytes, BytesMut};
use futures::{future::LocalBoxFuture, FutureExt};
use monoio::{
    buf::IoBufMut,
    io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt},
};

use crate::codec::{CodecError, Message, ZmqCodec};

const READ_BUF_SIZE: usize = 8 * 1024;

enum ReadHalf {
    #[cfg(feature = "tcp-transport")]
    Tcp(monoio::io::OwnedReadHalf<monoio::net::tcp::TcpStream>),
    #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
    Unix(monoio::io::OwnedReadHalf<monoio::net::unix::UnixStream>),
}

impl ReadHalf {
    #[cfg(feature = "tcp-transport")]
    pub fn tcp(half: monoio::io::OwnedReadHalf<monoio::net::tcp::TcpStream>) -> Self {
        Self::Tcp(half)
    }

    #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
    pub fn unix(half: monoio::io::OwnedReadHalf<monoio::net::unix::UnixStream>) -> Self {
        Self::Unix(half)
    }

    async fn read(self, mut buf: BytesMut) -> (Self, io::Result<usize>, BytesMut) {
        let len = buf.len();
        if len == buf.capacity() {
            buf.reserve(READ_BUF_SIZE);
        }
        let cap = buf.capacity();
        let buf = buf.slice_mut(len..cap);
        match self {
            #[cfg(feature = "tcp-transport")]
            Self::Tcp(mut half) => {
                let (res, buf) = half.read(buf).await;
                (Self::Tcp(half), res, buf.into_inner())
            }
            #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
            Self::Unix(mut half) => {
                let (res, buf) = half.read(buf).await;
                (Self::Unix(half), res, buf.into_inner())
            }
        }
    }
}

enum WriteHalf {
    #[cfg(feature = "tcp-transport")]
    Tcp(monoio::io::OwnedWriteHalf<monoio::net::tcp::TcpStream>),
    #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
    Unix(monoio::io::OwnedWriteHalf<monoio::net::unix::UnixStream>),
}

impl WriteHalf {
    #[cfg(feature = "tcp-transport")]
    pub fn tcp(half: monoio::io::OwnedWriteHalf<monoio::net::tcp::TcpStream>) -> Self {
        Self::Tcp(half)
    }

    #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
    pub fn unix(half: monoio::io::OwnedWriteHalf<monoio::net::unix::UnixStream>) -> Self {
        Self::Unix(half)
    }

    async fn write_all(self, buf: Bytes) -> (Self, io::Result<usize>, Bytes) {
        match self {
            #[cfg(feature = "tcp-transport")]
            Self::Tcp(mut half) => {
                let (res, buf) = half.write_all(buf).await;
                (Self::Tcp(half), res, buf)
            }
            #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
            Self::Unix(mut half) => {
                let (res, buf) = half.write_all(buf).await;
                (Self::Unix(half), res, buf)
            }
        }
    }

    async fn flush(self) -> (Self, io::Result<()>) {
        match self {
            #[cfg(feature = "tcp-transport")]
            Self::Tcp(mut half) => {
                let res = half.flush().await;
                (Self::Tcp(half), res)
            }
            #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
            Self::Unix(mut half) => {
                let res = half.flush().await;
                (Self::Unix(half), res)
            }
        }
    }

    async fn shutdown(self) -> (Self, io::Result<()>) {
        match self {
            #[cfg(feature = "tcp-transport")]
            Self::Tcp(mut half) => {
                let res = half.shutdown().await;
                (Self::Tcp(half), res)
            }
            #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
            Self::Unix(mut half) => {
                let res = half.shutdown().await;
                (Self::Unix(half), res)
            }
        }
    }
}

type ReadOp = LocalBoxFuture<'static, (ReadHalf, io::Result<usize>, BytesMut)>;
type WriteOp = LocalBoxFuture<'static, (WriteHalf, io::Result<usize>, Bytes)>;
type FlushOp = LocalBoxFuture<'static, (WriteHalf, io::Result<()>)>;

pub struct ZmqFramedRead {
    inner: Option<ReadHalf>,
    codec: ZmqCodec,
    decode_buf: BytesMut,
    read_op: Option<ReadOp>,
    eof: bool,
}

impl Unpin for ZmqFramedRead {}

impl ZmqFramedRead {
    fn new(inner: ReadHalf) -> Self {
        Self {
            inner: Some(inner),
            codec: ZmqCodec::new(),
            decode_buf: BytesMut::with_capacity(READ_BUF_SIZE),
            read_op: None,
            eof: false,
        }
    }
}

impl futures::Stream for ZmqFramedRead {
    type Item = Result<Message, CodecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.eof {
                return Poll::Ready(None);
            }

            if this.read_op.is_none() {
                match this.codec.decode(&mut this.decode_buf) {
                    Ok(Some(item)) => return Poll::Ready(Some(Ok(item))),
                    Ok(None) => {}
                    Err(err) => return Poll::Ready(Some(Err(err))),
                }

                let Some(inner) = this.inner.take() else {
                    return Poll::Pending;
                };
                let buf = std::mem::take(&mut this.decode_buf);
                this.read_op = Some(inner.read(buf).boxed_local());
            }

            let read_op = this.read_op.as_mut().expect("read op was just installed");
            match read_op.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready((inner, result, buf)) => {
                    this.read_op = None;
                    this.inner = Some(inner);
                    this.decode_buf = buf;
                    match result {
                        Ok(0) => {
                            this.eof = true;
                            return Poll::Ready(None);
                        }
                        Ok(n) => {
                            debug_assert!(n <= this.decode_buf.len());
                            continue;
                        }
                        Err(err) => {
                            return Poll::Ready(Some(Err(err.into())));
                        }
                    }
                }
            }
        }
    }
}

pub struct ZmqFramedWrite {
    inner: Option<WriteHalf>,
    codec: ZmqCodec,
    write_buf: BytesMut,
    write_op: Option<WriteOp>,
    flush_op: Option<FlushOp>,
    close_op: Option<FlushOp>,
    closed: bool,
}

impl Unpin for ZmqFramedWrite {}

impl ZmqFramedWrite {
    fn new(inner: WriteHalf) -> Self {
        Self {
            inner: Some(inner),
            codec: ZmqCodec::new(),
            write_buf: BytesMut::with_capacity(READ_BUF_SIZE),
            write_op: None,
            flush_op: None,
            close_op: None,
            closed: false,
        }
    }

    fn poll_write_buf(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), CodecError>> {
        loop {
            if let Some(write_op) = self.write_op.as_mut() {
                match write_op.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((inner, result, _buf)) => {
                        self.write_op = None;
                        self.inner = Some(inner);
                        result?;
                        continue;
                    }
                }
            }

            if self.write_buf.is_empty() {
                return Poll::Ready(Ok(()));
            }

            let Some(inner) = self.inner.take() else {
                return Poll::Pending;
            };
            let buf = self.write_buf.split().freeze();
            self.write_op = Some(inner.write_all(buf).boxed_local());
        }
    }

    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), CodecError>> {
        match self.poll_write_buf(cx)? {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => {}
        }

        if self.flush_op.is_none() {
            let Some(inner) = self.inner.take() else {
                return Poll::Pending;
            };
            self.flush_op = Some(inner.flush().boxed_local());
        }

        let flush_op = self.flush_op.as_mut().expect("flush op was just installed");
        match flush_op.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((inner, result)) => {
                self.flush_op = None;
                self.inner = Some(inner);
                Poll::Ready(result.map_err(Into::into))
            }
        }
    }
}

impl futures::Sink<&Message> for ZmqFramedWrite {
    type Error = CodecError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_write_buf(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: &Message) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.closed {
            return Err(CodecError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "framed writer is closed",
            )));
        }
        this.codec.encode(item, &mut this.write_buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_flush_inner(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Ok(()));
        }

        match this.poll_flush_inner(cx)? {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => {}
        }

        if this.close_op.is_none() {
            let Some(inner) = this.inner.take() else {
                return Poll::Pending;
            };
            this.close_op = Some(inner.shutdown().boxed_local());
        }

        let close_op = this.close_op.as_mut().expect("close op was just installed");
        match close_op.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((inner, result)) => {
                this.close_op = None;
                this.inner = Some(inner);
                result?;
                this.closed = true;
                Poll::Ready(Ok(()))
            }
        }
    }
}

pub struct FramedIo {
    pub read_half: ZmqFramedRead,
    pub write_half: ZmqFramedWrite,
}

impl FramedIo {
    #[cfg(feature = "tcp-transport")]
    pub fn new_tcp(
        read_half: monoio::io::OwnedReadHalf<monoio::net::tcp::TcpStream>,
        write_half: monoio::io::OwnedWriteHalf<monoio::net::tcp::TcpStream>,
    ) -> Self {
        Self::new(ReadHalf::tcp(read_half), WriteHalf::tcp(write_half))
    }

    #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
    pub fn new_unix(
        read_half: monoio::io::OwnedReadHalf<monoio::net::unix::UnixStream>,
        write_half: monoio::io::OwnedWriteHalf<monoio::net::unix::UnixStream>,
    ) -> Self {
        Self::new(ReadHalf::unix(read_half), WriteHalf::unix(write_half))
    }

    fn new(read_half: ReadHalf, write_half: WriteHalf) -> Self {
        Self {
            read_half: ZmqFramedRead::new(read_half),
            write_half: ZmqFramedWrite::new(write_half),
        }
    }

    pub fn into_parts(self) -> (ZmqFramedRead, ZmqFramedWrite) {
        (self.read_half, self.write_half)
    }
}
