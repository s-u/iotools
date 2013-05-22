ioreadBlock <- function(input, block=100000L) {
}

iostream <- function(expr, sep="|", input=file("stdin"), output=file("stdout"), block=100000L) {
}

as.output <- function(x) UseMethod("as.output")
as.output.default <- function(x) as.character(x)
as.output.table <- function(x) paste(names(x), x, sep='\t')
as.output.list <- function(x) paste(names(x), sapply(x, function (e) paste(as.character(e), collapse='|')), sep='\t')

run.chunked <- function(FUN) {
  load("stream.RData", .GlobalEnv);
  input <- file("stdin", "rb")
  output <- stdout()
  reader <- chunk.reader(input)
  while (TRUE) {
    chunk <- read.chunk(reader)
    if (!length(chunk)) break
    writeLines(as.output(FUN(chunk)), output)
  }
  invisible(TRUE)
}

run.map <- function() run.chunked(.GlobalEnv$map)
run.reduce <- function() run.chunked(.GlobalEnv$reduce)
