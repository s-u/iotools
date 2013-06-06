set.user <- function(name, gid=NA) .Call(C_setuser, name, gid)
