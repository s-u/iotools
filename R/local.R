# wrappers for using iotools locally; loads input, writes output, and
# operates on a file on the disk by localling walking through chunks
# of data and writing to disk; the latter somewhat replicates the
# chunk.apply functions and may eventually be removed

input <- function(file, formatter = .default.formatter, max.line = 65536L) {
  if (is.character(file)) {
    input = file(file, "rb")
    on.exit(close(input))
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
  }
  if (is.null(formatter.output)) formatter.output <- as.output

  writeLines(formatter.output(x), output)
  invisible(ret)
}

run.block = function(input, output = tempfile(), FUN = identity,
                  formatter=function(m) mstrsplit(m, "|")) {
  if (is.character(input)) {
    input = file(input, "rb")
    on.exit(close(input))
  }
  if (is.character(ret <- output)) {
    output = file(output, "wb")
    on.exit(close(output))
  }

  reader = chunk.reader(input)
  while (TRUE) {
    chunk = read.chunk(reader)
    if (!length(chunk)) break
    writeLines(as.output(FUN(formatter(chunk))), output)
  }
  invisible(ret)
}
