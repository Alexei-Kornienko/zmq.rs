use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::codec::{CodecError, Message, ZmqCodec};

use asynchronous_codec::{FramedRead, FramedWrite};
use futures::StreamExt;
use futures::{AsyncRead, AsyncWrite, SinkExt};

// Enables us to have multiple bounds on the dyn trait in `InnerFramed`
pub trait FrameableRead: AsyncRead + Unpin + Send + Sync {}
impl<T> FrameableRead for T where T: AsyncRead + Unpin + Send + Sync {}
pub trait FrameableWrite: AsyncWrite + Unpin + Send + Sync {}
impl<T> FrameableWrite for T where T: AsyncWrite + Unpin + Send + Sync {}

pub struct ZmqFramedRead {
    inner: asynchronous_codec::FramedRead<Box<dyn FrameableRead>, ZmqCodec>,
}

impl ZmqFramedRead {
    pub fn new(inner: asynchronous_codec::FramedRead<Box<dyn FrameableRead>, ZmqCodec>) -> Self {
        Self { inner }
    }
}

impl futures::Stream for ZmqFramedRead {
    type Item = Result<Message, CodecError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().inner.poll_next_unpin(cx)
    }
}

pub struct ZmqFramedWrite {
    inner: asynchronous_codec::FramedWrite<Box<dyn FrameableWrite>, ZmqCodec>,
}

impl ZmqFramedWrite {
    pub fn new(inner: asynchronous_codec::FramedWrite<Box<dyn FrameableWrite>, ZmqCodec>) -> Self {
        Self { inner }
    }
}

impl futures::Sink<Message> for ZmqFramedWrite {
    type Error = CodecError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.as_mut().inner.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_close_unpin(cx)
    }
}

/// Equivalent to [`asynchronous_codec::Framed<T, ZmqCodec>`]
pub struct FramedIo {
    pub read_half: ZmqFramedRead,
    pub write_half: ZmqFramedWrite,
}

impl FramedIo {
    pub fn new(read_half: Box<dyn FrameableRead>, write_half: Box<dyn FrameableWrite>) -> Self {
        let read_half = ZmqFramedRead::new(FramedRead::new(read_half, ZmqCodec::new()));
        let write_half = ZmqFramedWrite::new(FramedWrite::new(write_half, ZmqCodec::new()));
        Self {
            read_half,
            write_half,
        }
    }

    pub fn into_parts(self) -> (ZmqFramedRead, ZmqFramedWrite) {
        (self.read_half, self.write_half)
    }
}
