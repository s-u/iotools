\name{chunk}
\alias{chunk}
\alias{chunk.reader}
\alias{read.chunk}
\title{
  Functions for very fast chunk-wise processing
}
\description{
  \code{chunk.reader} creates a reader that will read from a binary
  connection in chunks while preserving integrity of lines.

  \code{read.chunk} reads the next chunk using the specified reader.
}
\usage{
chunk.reader(source, max.line = 65536L, sep = NULL)
read.chunk(reader, max.size = 33554432L, timeout = Inf)
}
\arguments{
  \item{source}{binary connection or character (which is interpreted as
    file name) specifying the source}
  \item{max.line}{maximum length of one line (in byets) - determines the
    size of the read buffer, default is 64kb}
  \item{sep}{optional string: key separator if key-aware chunking is to
    be used}
    character is considered a key and subsequent records holding the
    same key are guaranteed to be 
  \item{reader}{reader object as returned by \code{chunk.reader}}
  \item{max.size}{maximum size of the chunk (in bytes), default is 32Mb}
  \item{timeout}{numeric, timeout (in seconds) for reads if
  \code{source} is a raw file descriptor.}
}
\details{
  \code{chunk.reader} is essentially a filter that converts binary
  connection into chunks that can be subsequently parsed into data while
  preserving the integrity of input lines. \code{read.chunk} is used to
  read the actual chunks. The implementation is very thin to prevert
  copying of large vectors for best efficiency.

  If \code{sep} is set to a string, it is treated as a single-character
  separator character. If specified, prefix in the input up to the
  specified character is treated as a key and subsequent lines with the
  same key are guaranteed to be processed in the same chunk. Note that
  this implies that the chunk size is practically unlimited, since this
  may force accumulation of multiple chunks to satisfy this condition.
  Obviously, this increases the processing and memory overhead.

  In addition to connections \code{chunk.reader} supports raw file
  descriptors (integers of the class \code{"fileDescriptor"}). In that
  case the reads are preformed directly by \code{chunk.reader} and
  \code{timeout} can be used to perform non-blocking or timed
  reads (unix only, not supported on Windows).
}
\value{
  \code{chunk.reader} returns an object that can be used by
  \code{read.chunk}. If \code{source} is a string, it is equivalent to
  calling \code{chunk.reader(file(source, "rb"), ...)}.

  \code{read.chunk} returns a raw vector holding the next chunk or
  \code{NULL} if timeout was reached. It is deliberate that
  \code{read.chunk} does NOT return a character vector since that
  would reasult in a high performance penalty. Please use the
  appropriate parser to convert the chunk into data, see
  \code{\link{mstrsplit}}.
}
\author{
Simon Urbanek
}
%\examples{
%}
\keyword{manip}
