//! ICE-TCP framing: RFC 4571, as RFC 6544 uses it. Each packet is a 2-byte big-endian length,
//! then that many bytes. Both the SFU and the headless client use it.

use std::io::{self, ErrorKind, Read, Write};
use std::net::SocketAddr;

use mio::net::TcpStream;

/// The largest frame either side sends or takes. Larger than any packet str0m makes, and
/// the same as one UDP read.
pub(crate) const FRAME_MAX: usize = 2000;
/// Bytes waiting to go out on one stream, audio and everything else each. A packet that would
/// pass it is dropped, as UDP would lose it: past about 100 ms at a call's rates, a packet that
/// waits arrives too late to play.
const BACKLOG: usize = 32 * 1024;
/// One read from a stream.
const CHUNK: usize = 4096;

/// A frame length of zero or over the cap. The stream is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadFrame;

/// Splits a byte stream into frames.
#[derive(Debug, Default)]
pub struct Framer {
    buf: Vec<u8>,
    max: usize,
}

impl Framer {
    /// Frames of up to `max` bytes.
    pub fn new(max: usize) -> Self {
        Self {
            buf: Vec::new(),
            max,
        }
    }

    /// Adds bytes read from the stream.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The next whole frame, if one is buffered.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, BadFrame> {
        let Some(&[high, low]) = self.buf.get(..2) else {
            return Ok(None);
        };
        let len = usize::from(u16::from_be_bytes([high, low]));
        if len == 0 || len > self.max {
            return Err(BadFrame);
        }
        let Some(frame) = self.buf.get(2..2 + len) else {
            return Ok(None);
        };
        let frame = frame.to_vec();
        self.buf.drain(..2 + len);
        Ok(Some(frame))
    }
}

/// Appends `packet` to `out` as one frame. False if it is empty, too big, or `out` is full.
pub fn encode(packet: &[u8], out: &mut Vec<u8>) -> bool {
    let Ok(len) = u16::try_from(packet.len()) else {
        return false;
    };
    if len == 0 || packet.len() > FRAME_MAX || out.len() + 2 + packet.len() > BACKLOG {
        return false;
    }
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(packet);
    true
}

/// How a read ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Readout {
    /// Nothing more to read for now.
    Drained,
    /// The read budget ran out with bytes still waiting. Read again soon.
    More,
    /// The peer closed, the stream failed, or it sent a bad frame.
    Closed,
}

/// What waits to go out on a stream. Audio passes everything else, but only between frames:
/// a frame started on the wire must finish, or the stream's framing is lost.
#[derive(Debug, Default)]
struct Backlog {
    urgent: Vec<u8>,
    bulk: Vec<u8>,
    /// Whole frames on their way, of which the first `written` bytes have gone.
    writing: Vec<u8>,
    written: usize,
}

impl Backlog {
    /// Queues one packet as a frame. False if it was dropped.
    fn push(&mut self, packet: &[u8], urgent: bool) -> bool {
        let queue = if urgent {
            &mut self.urgent
        } else {
            &mut self.bulk
        };
        encode(packet, queue)
    }

    /// The bytes to write next, starting the next frames once the last have gone. Empty once
    /// nothing is waiting.
    fn next(&mut self) -> &[u8] {
        if self.written == self.writing.len() {
            self.writing.clear();
            self.written = 0;
            if !self.urgent.is_empty() {
                std::mem::swap(&mut self.writing, &mut self.urgent);
            } else if let [high, low, ..] = self.bulk[..] {
                let frame = 2 + usize::from(u16::from_be_bytes([high, low]));
                self.writing.extend(self.bulk.drain(..frame));
            }
        }
        self.writing.get(self.written..).unwrap_or_default()
    }

    /// `n` bytes of `next` went out.
    fn wrote(&mut self, n: usize) {
        self.written += n;
    }
}

/// One TCP stream: framed reads and a bounded write backlog.
pub(crate) struct Conn {
    pub socket: TcpStream,
    pub peer: SocketAddr,
    framer: Framer,
    backlog: Backlog,
}

impl Conn {
    pub fn new(socket: TcpStream, peer: SocketAddr) -> Self {
        Self {
            socket,
            peer,
            framer: Framer::new(FRAME_MAX),
            backlog: Backlog::default(),
        }
    }

    /// Reads up to `budget` bytes and hands each whole frame to `frame`, which returns false
    /// to close the stream.
    pub fn read(&mut self, budget: usize, mut frame: impl FnMut(&[u8]) -> bool) -> Readout {
        let mut buf = [0; CHUNK];
        let mut taken = 0;
        loop {
            if taken >= budget {
                return Readout::More;
            }
            let n = match self.socket.read(&mut buf) {
                Ok(0) => return Readout::Closed,
                Ok(n) => n,
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Readout::Drained,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return Readout::Closed,
            };
            taken += n;
            let Some(bytes) = buf.get(..n) else {
                return Readout::Closed;
            };
            self.framer.push(bytes);
            loop {
                match self.framer.next_frame() {
                    Ok(Some(packet)) => {
                        if !frame(&packet) {
                            return Readout::Closed;
                        }
                    }
                    Ok(None) => break,
                    Err(BadFrame) => return Readout::Closed,
                }
            }
        }
    }

    /// Queues one packet, ahead of the others if `urgent`. False if it was dropped.
    pub fn queue(&mut self, packet: &[u8], urgent: bool) -> bool {
        self.backlog.push(packet, urgent)
    }

    /// Writes what it can. `Ok(true)` once nothing is waiting.
    pub fn flush(&mut self) -> io::Result<bool> {
        loop {
            let bytes = self.backlog.next();
            if bytes.is_empty() {
                return Ok(true);
            }
            match self.socket.write(bytes) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(n) => self.backlog.wrote(n),
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(false),
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                // Not connected yet: a client's connect is still in flight.
                Err(error) if error.kind() == ErrorKind::NotConnected => return Ok(false),
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{encode, Backlog, BadFrame, Framer, BACKLOG, FRAME_MAX};

    /// Takes everything from `backlog` in `step`-byte writes, and returns the frames in order.
    fn sent(backlog: &mut Backlog, step: usize) -> Vec<Vec<u8>> {
        let mut framer = Framer::new(FRAME_MAX);
        let mut frames = Vec::new();
        loop {
            let bytes = backlog.next();
            let n = bytes.len().min(step);
            if n == 0 {
                return frames;
            }
            framer.push(&bytes[..n]);
            backlog.wrote(n);
            while let Ok(Some(frame)) = framer.next_frame() {
                frames.push(frame);
            }
        }
    }

    /// As `sent`, with the first `skip` bytes, the rest of a frame already started, left out.
    fn sent_after(backlog: &mut Backlog, skip: usize) -> Vec<Vec<u8>> {
        let bytes = backlog.next();
        assert_eq!(bytes.len(), skip, "the frame started goes first");
        backlog.wrote(skip);
        sent(backlog, 3)
    }

    #[test]
    fn audio_goes_ahead_of_what_waits_but_not_into_a_frame() {
        let mut backlog = Backlog::default();
        assert!(backlog.push(b"video one", false));
        assert!(backlog.push(b"video two", false));
        // The first frame has started: two bytes of it are on the wire.
        let started = backlog.next();
        assert_eq!(started.len(), 11);
        backlog.wrote(2);
        assert!(backlog.push(b"audio", true));
        let mut out = sent_after(&mut backlog, 9);
        // The rest of the first frame, whose head went out above, cannot be read as a frame
        // here; what follows is whole, with audio first.
        assert_eq!(out.pop(), Some(b"video two".to_vec()));
        assert_eq!(out.pop(), Some(b"audio".to_vec()));
    }

    #[test]
    fn each_queue_is_bounded_alone() {
        let mut backlog = Backlog::default();
        let packet = [7; 1000];
        let fits = BACKLOG / (packet.len() + 2);
        for _ in 0..fits {
            assert!(backlog.push(&packet, false));
        }
        assert!(!backlog.push(&packet, false), "video past the cap");
        assert!(backlog.push(&packet, true), "video cannot crowd out audio");
        assert_eq!(sent(&mut backlog, 700).len(), fits + 1);
    }

    #[test]
    fn frames_split_across_reads_come_out_whole() {
        let mut out = Vec::new();
        assert!(encode(b"hello", &mut out));
        assert!(encode(b"there", &mut out));
        let mut framer = Framer::new(FRAME_MAX);
        let (first, rest) = out.split_at(4);
        framer.push(first);
        assert_eq!(framer.next_frame(), Ok(None));
        framer.push(rest);
        assert_eq!(framer.next_frame(), Ok(Some(b"hello".to_vec())));
        assert_eq!(framer.next_frame(), Ok(Some(b"there".to_vec())));
        assert_eq!(framer.next_frame(), Ok(None));
    }

    #[test]
    fn an_empty_or_oversized_frame_is_refused() {
        let mut framer = Framer::new(10);
        framer.push(&[0, 0]);
        assert_eq!(framer.next_frame(), Err(BadFrame));
        let mut framer = Framer::new(10);
        framer.push(&[0, 11]);
        assert_eq!(framer.next_frame(), Err(BadFrame));
        assert!(!encode(&[], &mut Vec::new()));
        assert!(!encode(&[0; FRAME_MAX + 1], &mut Vec::new()));
    }
}
