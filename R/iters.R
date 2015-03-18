
idstrsplit <- function(x, col_types, sep="|", nsep=NA, strict=TRUE, 
                       max.line = 65536L, max.size = 33554432L) {
  cr <- chunk.reader(source=x, max.line=max.line)
  nextEl <- function() {
    chunk <- read.chunk(cr, max.size=max.size)
    if (length(chunk) == 0)
      stop("StopIteration", call. = FALSE)
    dstrsplit(chunk, col_types=col_types, sep=sep, nsep=nsep, strict=strict)
  }
  it <- list(nextElem=nextEl)
  class(it) <- c("abstractiter", "iter", "dstrsplit_iter")
  it
}

imstrsplit <- function(x, sep="|", nsep=NA, strict=TRUE, ncol = NA,
                       type=c("character", "numeric", "logical", "integer",  
                              "complex", "raw"),
                       max.line = 65536L, max.size = 33554432L) {
  cr <- chunk.reader(source=x, max.line=max.line)
  nextEl <- function() {
    chunk <- read.chunk(cr, max.size=max.size)
    if (length(chunk) == 0)
      stop("StopIteration", call. = FALSE)
    mstrsplit(chunk, sep=sep, strict=strict, ncol=ncol, type=type, nsep=nsep)
  }
  it <- list(nextElem=nextEl)
  class(it) <- c("abstractiter", "iter", "mstrsplit_iter")
  it
}
