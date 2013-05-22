readSTDIN <- function(n = 10000L)
  .Call(stdin_read, n)
