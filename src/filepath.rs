use std::{
    fs::{self, File, Permissions},
    io,
    os::unix::fs::PermissionsExt,
    path::Path,
};
/*
默认权限：当程序创建一个文件时，会请求一个“最大”权限。在 Unix 系统上，Rust 的默认请求权限通常是：
文件：0o666（rw-rw-rw-，所有人可读写）
目录：0o777（rwxrwxrwx，所有人可读写执行）
应用掩码：操作系统的 umask 会从上述默认权限中移除对应的位，得到最终的权限
这里为了实现精确控制，采用先创建之后再手动修改set_permissions
*/
// TODO: 增加一个参数，确认如果存在文件/目录是否报错返回
pub fn create_shared_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path)?; // create_dir不递归创建多层目录
    fs::set_permissions(path, Permissions::from_mode(0o777))
}
pub fn create_shared_file(path: &Path) -> io::Result<File> {
    // 对于文件，如果已经存在则直接报错
    let file = File::options().write(true).create_new(true).open(path)?;
    file.set_permissions(Permissions::from_mode(0o666))?;
    Ok(file)
}
