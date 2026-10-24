use std::fs::{self, File, Permissions};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
/*
Default permissions: creation requests a maximum set of permissions. On Unix, Rust usually requests:
Files: 0o666 (rw-rw-rw-, readable and writable by everyone).
Directories: 0o777 (rwxrwxrwx, readable, writable, and traversable by everyone).
The operating system's umask removes bits from these defaults to determine the final permissions.
For precise control, create the entry first and then explicitly apply set_permissions.
*/
// TODO: add an option controlling whether an existing file/directory should cause an error.
pub fn create_shared_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path)?; // create_dir does not create parent directories recursively.
    fs::set_permissions(path, Permissions::from_mode(0o777))
}
pub fn create_shared_file(path: &Path) -> io::Result<File> {
    // Return an error if the file already exists.
    let file = File::options().write(true).create_new(true).open(path)?;
    file.set_permissions(Permissions::from_mode(0o666))?;
    Ok(file)
}
