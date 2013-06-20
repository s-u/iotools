set.user <- function(name, gid=NA) .Call(C_setuser, name, gid)
set.tempdir <- function(path) invisible(.Call(C_setTempDir, path))
