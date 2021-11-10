use crate::types::{NaiveError, Result};
use std::io::{Read, Write};
use std::net::TcpStream;

type MessageLengthType = u32;

const N_BYTES_MESSAGE_LENGTH: usize = (MessageLengthType::BITS as usize) >> 3;

pub fn send_message_to_stream<Message: protobuf::Message>(
    message: &Message,
    stream: &mut TcpStream,
) -> Result<Vec<u8>> {
    let bytes = message.write_to_bytes()?;
    // Write the length of the message followed by the message content.
    stream.write(&(bytes.len() as MessageLengthType).to_be_bytes())?;
    stream.write(&bytes)?;
    Ok(bytes)
}

pub fn get_message_from_stream<Message: protobuf::Message>(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    bytes: &mut Vec<u8>,
) -> Result<Message> {
    let mut message_length_bytes = [0u8; N_BYTES_MESSAGE_LENGTH];
    let mut message_length_bytes_index = 0;
    let mut message_length = None;
    bytes.clear();
    while let Ok(num_bytes) = stream.read(buffer) {
        for i in 0..num_bytes {
            if message_length.is_none() {
                // Determine the length of the message in the first few bytes.
                message_length_bytes[message_length_bytes_index] = buffer[i];
                message_length_bytes_index += 1;
                if message_length_bytes_index == N_BYTES_MESSAGE_LENGTH {
                    message_length =
                        Some(MessageLengthType::from_be_bytes(message_length_bytes) as usize);
                }
            } else {
                // Read the message content.
                bytes.push(buffer[i]);
            }
        }
        if bytes.len() >= message_length.unwrap_or(usize::MAX) {
            // Have reached the end of the message.
            return Ok(Message::parse_from_bytes(&bytes)?);
        }
    }
    Err(NaiveError::TcpReadError)
}
