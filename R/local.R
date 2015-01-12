# wrappers for using iotools locally; loads input, writes output,

input.file <- function(file_name, formatter = mstrsplit, ...) {
  if (is.character(file_name)) {
    input = file(file_name, "rb")
    on.exit(close(input))
  } else {
    stop("'file_name' must be a character string to a file path.")
  }

  n = file.info(file_name)$size
  formatter(readBin(input, what="raw", n=n), ...)
}

output.file <- function(x, file, formatter.output = NULL) {
  if (is.character(ret <- file)) {
    output = file(file, "wb")
    on.exit(close(output))
  } else if (inherits(file, "connection")) {
    output = file
  } else {
    stop("'file' must be a connection or a character string to a file path.")
  }
  if (is.null(formatter.output)) formatter.output <- as.output

  writeLines(formatter.output(x), output)
  invisible(ret)
}
