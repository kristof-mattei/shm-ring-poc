//! Setup over the Unix socket: the memfd travels as `SCM_RIGHTS`, then the socket only signals liveness.

use std::io::{self, IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{BorrowedFd, OwnedFd};

use rustix::cmsg_space;
use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags, recvmsg, sendmsg,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, Interest};
use tokio::net::UnixStream;

const HELLO: u8 = b'H';
const ACK: u8 = b'A';

/// Sends `fd` with a one-byte hello.
pub(crate) async fn send_fd(stream: &UnixStream, fd: BorrowedFd<'_>) -> io::Result<()> {
    stream
        .async_io(Interest::WRITABLE, || {
            let fds = [fd];
            let mut space = [MaybeUninit::uninit(); cmsg_space!(ScmRights(1))];
            let mut control = SendAncillaryBuffer::new(&mut space);

            let pushed = control.push(SendAncillaryMessage::ScmRights(&fds));
            assert!(pushed, "`space` holds exactly one `ScmRights(1)` message");

            let sent = sendmsg(
                stream,
                &[IoSlice::new(&[HELLO])],
                &mut control,
                SendFlags::NOSIGNAL,
            )?;

            if sent == 1 {
                Ok(())
            } else {
                Err(io::Error::from(io::ErrorKind::WriteZero))
            }
        })
        .await
}

/// Receives the fd sent by [`send_fd`].
pub(crate) async fn recv_fd(stream: &UnixStream) -> io::Result<OwnedFd> {
    stream
        .async_io(Interest::READABLE, || {
            let mut byte = [0_u8; 1];
            let mut space = [MaybeUninit::uninit(); cmsg_space!(ScmRights(1))];
            let mut control = RecvAncillaryBuffer::new(&mut space);

            let received = recvmsg(
                stream,
                &mut [IoSliceMut::new(&mut byte)],
                &mut control,
                RecvFlags::CMSG_CLOEXEC,
            )?;

            if received.bytes == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }

            if byte != [HELLO] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "handshake does not start with a hello",
                ));
            }

            for message in control.drain() {
                if let RecvAncillaryMessage::ScmRights(mut fds) = message {
                    if let Some(fd) = fds.next() {
                        return Ok(fd);
                    }
                }
            }

            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "hello arrived without an fd",
            ))
        })
        .await
}

pub(crate) async fn send_ack(stream: &mut UnixStream) -> io::Result<()> {
    stream.write_all(&[ACK]).await
}

pub(crate) async fn recv_ack(stream: &mut UnixStream) -> io::Result<()> {
    let mut byte = [0_u8; 1];

    stream.read_exact(&mut byte).await?;

    if byte == [ACK] {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer did not acknowledge the region",
        ))
    }
}

/// Resolves when the peer closes its end. The protocol sends nothing after the handshake, so any byte is an error.
pub(crate) async fn closed(stream: &mut UnixStream) -> io::Result<()> {
    let mut byte = [0_u8; 1];

    match stream.read(&mut byte).await? {
        0 => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer sent data after the handshake",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;
    use tokio::net::UnixStream;

    use super::{closed, recv_ack, recv_fd, send_ack, send_fd};
    use crate::region::Region;

    #[tokio::test]
    async fn fd_crosses_the_socket_and_maps_the_same_memory() {
        let (mut client, mut server) = UnixStream::pair().expect("socket pair is created");

        let created = Arc::new(Region::create(8, 8).expect("region is created"));

        send_fd(&client, created.fd()).await.expect("fd is sent");

        let fd = recv_fd(&server).await.expect("fd is received");
        let opened = Arc::new(Region::open(fd).expect("received fd maps"));

        send_ack(&mut server).await.expect("ack is sent");
        recv_ack(&mut client).await.expect("ack is received");

        let (mut requests, _responses) = created.client_endpoints(0);
        let (mut received, _response_producer) = opened.server_endpoints(0);

        let mut slot = requests.reserve().expect("ring has room");

        slot.set_id(42);
        slot.commit();

        assert_eq!(
            received.recv().expect("message crosses the mappings").id(),
            42
        );

        drop(client);

        closed(&mut server)
            .await
            .expect("dropping the client end reads as EOF");
    }
}
