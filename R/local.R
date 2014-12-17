# wrappers for using iotools locally; loads input, writes output,

input <- function(file, formatter = .default.formatter, max.line = 65536L) {
  if (is.character(file)) {
    input = file(file, "rb")
    on.exit(close(input))
  } else if (inherits(file, "connection")) {
    input = file
  } else {
    stop("'file' must be a connection or a character string to a file path.")
  }
  reader = chunk.reader(input, max.line=max.line)
  output = NULL
  while (TRUE) {
    chunk = read.chunk(reader)
    if (!length(chunk)) break
    output = rbind(output, formatter(chunk))
  }
  return(output)
}

output <- function(x, file, formatter.output = NULL) {
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
