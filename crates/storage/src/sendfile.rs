//! Zero-copy (or near-zero-copy) file-to-socket send helper.
//!
//! ## Architecture
//! `sendfile_range()` is the single public entry point.  It sends `len` bytes
//! from `path` at byte `offset` to `socket`.
//!
//! ## Platform strategy
//! - **Linux (future)**: When the `nix` crate is added to the dependency tree,
//!   replace the fallback body with `nix::sys::sendfile::sendfile()` which
//!   exposes `sendfile(2)` as safe Rust, keeping `#![forbid(unsafe_code)]`.
//! - **macOS / all other platforms**: `pread(2)` + `write_all`.  macOS's
//!   `sendfile(2)` has different semantics and is not yet wrapped here.
//!
//! **TLS note**: never call this on a TLS-wrapped socket.  The kernel does not
//! understand TLS record boundaries; use the caller's own buffered write path.

#![forbid(unsafe_code)]

use std::io::{self, Write};
use std::net::TcpStream;
use std::path::Path;

/// Send `len` bytes from `path` starting at byte `offset` into `socket`.
///
/// Returns `len` on success.  Returns `0` immediately when `len == 0`.
///
/// Currently uses `pread`/`read_exact_at` + `write_all` on all platforms.
/// The function signature is stable so callers require no changes when a
/// native `sendfile` path is added for Linux.
pub fn sendfile_range(socket: &TcpStream, path: &Path, offset: u64, len: u64) -> io::Result<u64> {
    if len == 0 {
        return Ok(0);
    }
    pread_write(socket, path, offset, len)
}

// ── pread + write_all fallback (all platforms) ────────────────────────────────

fn pread_write(
    socket: &TcpStream,
    path: &Path,
    offset: u64,
    len: u64,
) -> io::Result<u64> {
    use std::fs::File;
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;

    let file = File::open(path)?;
    let len_usize = usize::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "len exceeds usize::MAX"))?;

    // For small payloads avoid a heap allocation.
    const INLINE_BUF: usize = 64 * 1024;

    let mut sock: &TcpStream = socket;

    if len_usize <= INLINE_BUF {
        let mut buf = [0_u8; INLINE_BUF];
        let slice = &mut buf[..len_usize];
        read_at(&file, path, slice, offset)?;
        sock.write_all(slice)?;
    } else {
        let mut buf = vec![0_u8; len_usize];
        read_at(&file, path, &mut buf, offset)?;
        sock.write_all(&buf)?;
    }

    Ok(len)
}

/// Platform-agnostic positional read (no cursor mutation).
#[cfg(unix)]
fn read_at(file: &std::fs::File, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
        .map_err(|e| io::Error::new(e.kind(), format!("pread {}: {e}", path.display())))
}

#[cfg(not(unix))]
fn read_at(file: &std::fs::File, _path: &Path, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = file
        .try_clone()
        .map_err(|e| io::Error::new(e.kind(), format!("try_clone: {e}")))?;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;

    struct TempDir {
        pub path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let path = std::env::temp_dir().join(format!(
                "rafka-sendfile-{label}-{}-{millis}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn sendfile_range_sends_exact_bytes() {
        let data = b"Hello, sendfile world! 0123456789";
        let tmp = TempDir::new("exact");
        let file_path = tmp.path.join("data.bin");
        std::fs::write(&file_path, data).expect("write tmp file");

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let sender = TcpStream::connect(addr).expect("connect");
        let (mut receiver, _) = listener.accept().expect("accept");

        let offset = 7_u64;
        let len = 8_u64; // "sendfile"
        sendfile_range(&sender, &file_path, offset, len).expect("sendfile_range");
        drop(sender);

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).expect("read");
        assert_eq!(&received, b"sendfile");
    }

    #[test]
    fn sendfile_range_zero_len_is_noop() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let sender = TcpStream::connect(addr).expect("connect");
        let (mut receiver, _) = listener.accept().expect("accept");

        // /dev/null exists on Unix; use a valid path that won't actually be read.
        let tmp = TempDir::new("zero-len");
        let path = tmp.path.join("placeholder");
        std::fs::write(&path, b"x").unwrap();

        let result = sendfile_range(&sender, &path, 0, 0);
        assert!(result.is_ok());
        drop(sender);

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).expect("read");
        assert!(received.is_empty());
    }

    #[test]
    fn sendfile_range_full_file() {
        let data: Vec<u8> = (0_u8..=255_u8).collect();
        let tmp = TempDir::new("full");
        let file_path = tmp.path.join("full.bin");
        std::fs::write(&file_path, &data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let sender = TcpStream::connect(addr).expect("connect");
        let (mut receiver, _) = listener.accept().expect("accept");

        sendfile_range(&sender, &file_path, 0, data.len() as u64).expect("sendfile_range");
        drop(sender);

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).expect("read");
        assert_eq!(received, data);
    }
}
