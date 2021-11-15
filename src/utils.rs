use crate::types::Result;

/// Use an architecture-independent type to serialize the chunk size.
type ChunkLengthType = u32;

const N_BYTES_CHUNK_LENGTH: usize = (ChunkLengthType::BITS as usize) >> 3;

pub fn read_chunk(reader: &mut impl std::io::Read, buffer: &mut Vec<u8>) -> Result<usize> {
    buffer.clear();
    let chunk_length = read_chunk_length(reader)?;
    buffer.resize(chunk_length, 0u8);
    reader.read_exact(buffer)?;
    Ok(chunk_length)
}

fn read_chunk_length(reader: &mut impl std::io::Read) -> Result<usize> {
    let mut buffer = [0u8; N_BYTES_CHUNK_LENGTH];
    match reader.read_exact(&mut buffer) {
        Ok(()) => (),
        Err(error) => {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(0usize);
            }
            return Err(error.into());
        }
    }
    Ok(ChunkLengthType::from_be_bytes(buffer) as usize)
}

pub fn write_chunk(writer: &mut impl std::io::Write, bytes: &[u8]) -> Result<()> {
    // Write the message length followed by the message content.
    writer.write(&(bytes.len() as ChunkLengthType).to_be_bytes())?;
    writer.write(&bytes)?;
    writer.flush()?;
    Ok(())
}

/// Read a chunk that consists of a single message.
pub fn read_message<Message: protobuf::Message, Reader: std::io::Read>(
    reader: &mut Reader,
) -> Result<Option<Message>> {
    let mut bytes = Vec::new();
    let num_bytes = read_chunk(reader, &mut bytes)?;
    if num_bytes == 0 {
        return Ok(None);
    }
    Ok(Some(Message::parse_from_bytes(&bytes)?))
}

/// Write a chunk that consists of a single message.
pub fn write_message<Message: protobuf::Message, Writer: std::io::Write>(
    message: &Message,
    writer: &mut Writer,
) -> Result<()> {
    write_chunk(writer, &message.write_to_bytes()?)
}

pub fn try_remove_file(path: &std::path::Path) -> Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) => {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error.into());
            }
            Ok(false)
        }
    }
}
