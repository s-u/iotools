readSTDIN <- function(n = 10000L)
  .Call(stdin_read, n)

writeBIN <- function(what, flush=TRUE)
  .Call(stdout_writeBin, what, flush)
