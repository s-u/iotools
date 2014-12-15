.onAttach <- function(libname, pkgname) {
  ## this is for compatibility -- hmr used to be part of iotools so some scripts may require that
  if (nzchar(Sys.getenv("IOTOOLS_AUTOLOAD_HMR"))) do.call("require", list("hmr"))
}
