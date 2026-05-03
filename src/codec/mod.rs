//! Implements a codec for ZMQ, providing a way to convert from byte-oriented
//! IO to a protocol comprised of [`Message`] frames.

mod command;
mod error;
mod greeting;
pub(crate) mod mechanism;
mod zmq_codec;

pub(crate) use command::{ZmqCommand, ZmqCommandName};
pub(crate) use error::{CodecError, CodecResult};
pub(crate) use greeting::{ZmqGreeting, ZmtpVersion};
pub use zmq_codec::ZmqCodec;

use crate::message::ZmqMessage;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
pub enum Message {
    Greeting(ZmqGreeting),
    Command(ZmqCommand),
    Message(ZmqMessage),
}
