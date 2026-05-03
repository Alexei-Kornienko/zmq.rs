use std::{
    ffi::c_void,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use asynchronous_codec::{Decoder, Encoder};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{future::LocalBoxFuture, FutureExt};
use monoio::{
    buf::IoBufMut,
    io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt},
};

use crate::codec::{CodecError, Message, ZmqCodec};

const READ_BUF_SIZE: usize = 8 * 1024;
const LARGE_WRITE_THRESHOLD: usize = 4 * 1024;
const HEADER_RESERVE: usize = 16;
const MAX_FRAME_HEADER_LEN: usize = 9;

struct LargeWriteBufs {
    headers: BytesMut,
    bodies: Vec<Bytes>,
    #[cfg(unix)]
    iovecs: Vec<libc::iovec>,
}

impl LargeWriteBufs {
    fn new() -> Self {
        Self {
            headers: BytesMut::with_capacity(MAX_FRAME_HEADER_LEN * HEADER_RESERVE),
            bodies: Vec::with_capacity(HEADER_RESERVE),
            #[cfg(unix)]
            iovecs: Vec::with_capacity(HEADER_RESERVE * 2),
        }
    }

    fn clear(&mut self) {
        self.headers.clear();
        self.bodies.clear();
        #[cfg(unix)]
        self.iovecs.clear();
    }
}

enum PendingWrite {
    Small(Bytes),
    #[cfg(unix)]
    Large(LargeWriteBufs),
}

fn frame_header_len(body_len: usize) -> usize {
    if body_len > 255 {
        9
    } else {
        2
    }
}

fn encode_frame_header(body_len: usize, more: bool, dst: &mut BytesMut) {
    let mut flags: u8 = 0;
    if more {
        flags |= 0b0000_0001;
    }
    if body_len > 255 {
        flags |= 0b0000_0010;
    }

    dst.put_u8(flags);
    if body_len > 255 {
        dst.put_u64(body_len as u64);
    } else {
        dst.put_u8(body_len as u8);
    }
}

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

    #[cfg(unix)]
    async fn write_vectored_all(
        self,
        bufs: LargeWriteBufs,
    ) -> (Self, io::Result<usize>, LargeWriteBufs) {
        match self {
            #[cfg(feature = "tcp-transport")]
            Self::Tcp(mut half) => {
                let LargeWriteBufs {
                    headers,
                    bodies,
                    iovecs,
                } = bufs;
                let (res, iovecs) = half.write_vectored_all(iovecs).await;
                (
                    Self::Tcp(half),
                    res,
                    LargeWriteBufs {
                        headers,
                        bodies,
                        iovecs,
                    },
                )
            }
            #[cfg(all(feature = "ipc-transport", target_family = "unix"))]
            Self::Unix(mut half) => {
                let LargeWriteBufs {
                    headers,
                    bodies,
                    iovecs,
                } = bufs;
                let (res, iovecs) = half.write_vectored_all(iovecs).await;
                (
                    Self::Unix(half),
                    res,
                    LargeWriteBufs {
                        headers,
                        bodies,
                        iovecs,
                    },
                )
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
type WriteOp = LocalBoxFuture<'static, (WriteHalf, io::Result<usize>, PendingWrite)>;
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
    large_bufs: Option<LargeWriteBufs>,
    #[cfg(unix)]
    pending_large: Option<LargeWriteBufs>,
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
            large_bufs: Some(LargeWriteBufs::new()),
            #[cfg(unix)]
            pending_large: None,
            write_op: None,
            flush_op: None,
            close_op: None,
            closed: false,
        }
    }

    #[cfg(unix)]
    fn try_start_large_send(&mut self, item: &Message) -> Result<bool, CodecError> {
        let Message::Message(message) = item else {
            return Ok(false);
        };

        let body_len: usize = message.iter().map(Bytes::len).sum();
        if body_len < LARGE_WRITE_THRESHOLD {
            return Ok(false);
        }

        if self.pending_large.is_some() || !self.write_buf.is_empty() {
            return Err(CodecError::Other(
                "framed writer accepted a message before it was ready",
            ));
        }

        let frame_count = message.len();
        let required_headers = MAX_FRAME_HEADER_LEN * frame_count;
        let required_iovecs = frame_count * 2;

        let mut bufs = self
            .large_bufs
            .take()
            .ok_or(CodecError::Other("large write buffer is already in use"))?;
        bufs.clear();
        if bufs.headers.capacity() < required_headers {
            bufs.headers.reserve(required_headers);
        }
        if bufs.bodies.capacity() < frame_count {
            bufs.bodies.reserve(frame_count);
        }
        if bufs.iovecs.capacity() < required_iovecs {
            bufs.iovecs.reserve(required_iovecs);
        }

        for (idx, frame) in message.iter().enumerate() {
            encode_frame_header(frame.len(), idx + 1 != frame_count, &mut bufs.headers);
            bufs.bodies.push(frame.clone());
        }

        let mut header_offset = 0;
        for body in &bufs.bodies {
            let header_len = frame_header_len(body.len());
            bufs.iovecs.push(libc::iovec {
                iov_base: bufs.headers.as_ptr().wrapping_add(header_offset) as *mut c_void,
                iov_len: header_len,
            });
            bufs.iovecs.push(libc::iovec {
                iov_base: body.as_ptr() as *mut c_void,
                iov_len: body.len(),
            });
            header_offset += header_len;
        }

        self.pending_large = Some(bufs);
        Ok(true)
    }

    #[cfg(not(unix))]
    fn try_start_large_send(&mut self, _item: &Message) -> Result<bool, CodecError> {
        Ok(false)
    }

    fn poll_write_buf(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), CodecError>> {
        loop {
            if let Some(write_op) = self.write_op.as_mut() {
                match write_op.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((inner, result, pending)) => {
                        self.write_op = None;
                        self.inner = Some(inner);
                        match pending {
                            PendingWrite::Small(_buf) => {}
                            #[cfg(unix)]
                            PendingWrite::Large(mut bufs) => {
                                bufs.clear();
                                self.large_bufs = Some(bufs);
                            }
                        }
                        result?;
                        continue;
                    }
                }
            }

            let Some(inner) = self.inner.take() else {
                return Poll::Pending;
            };

            if !self.write_buf.is_empty() {
                let buf = self.write_buf.split().freeze();
                self.write_op = Some(
                    async move {
                        let (inner, result, buf) = inner.write_all(buf).await;
                        (inner, result, PendingWrite::Small(buf))
                    }
                    .boxed_local(),
                );
                continue;
            }

            #[cfg(unix)]
            if let Some(bufs) = self.pending_large.take() {
                self.write_op = Some(
                    async move {
                        let (inner, result, bufs) = inner.write_vectored_all(bufs).await;
                        (inner, result, PendingWrite::Large(bufs))
                    }
                    .boxed_local(),
                );
                continue;
            }

            self.inner = Some(inner);
            return Poll::Ready(Ok(()));
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
        if this.try_start_large_send(item)? {
            return Ok(());
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
