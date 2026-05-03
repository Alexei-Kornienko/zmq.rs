use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::codec::{CodecError, Message};

pub struct ZmqFramedRead;

impl futures::Stream for ZmqFramedRead {
    type Item = Result<Message, CodecError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("monoio framed read is not implemented")
    }
}

pub struct ZmqFramedWrite;

impl futures::Sink<&Message> for ZmqFramedWrite {
    type Error = CodecError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!("monoio framed write readiness is not implemented")
    }

    fn start_send(self: Pin<&mut Self>, _item: &Message) -> Result<(), Self::Error> {
        todo!("monoio framed write send is not implemented")
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!("monoio framed write flush is not implemented")
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!("monoio framed write close is not implemented")
    }
}

pub struct FramedIo {
    pub read_half: ZmqFramedRead,
    pub write_half: ZmqFramedWrite,
}

impl FramedIo {
    pub fn new<R, W>(_read_half: R, _write_half: W) -> Self {
        todo!("monoio framed IO is not implemented")
    }

    pub fn into_parts(self) -> (ZmqFramedRead, ZmqFramedWrite) {
        (self.read_half, self.write_half)
    }
}
