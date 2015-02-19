.onAttach <- function(libname, pkgname) {
  ## this is for compatibility -- hmr used to be part of iotools so some scripts may require that
  ## we make this default by now but will deprecate it eventually
  tryCatch(do.call("require", list("hmr",quietly=TRUE)), error=identity, warning=identity)
}
