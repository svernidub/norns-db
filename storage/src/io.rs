use std::{fs::File, io};

/// Reads `buf.len()` bytes from `file` at the given byte `offset`
/// without modifying the file's cursor position.
///
/// Uses `pread` on Unix and `seek_read` on Windows.
pub fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    tracing::trace!(offset, len = buf.len(), "read_at");

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buf, offset)
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(buf, offset)
    }
}
