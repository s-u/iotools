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
chunk.reader(source, max.line = 65536L)
read.chunk(reader, max.size = 33554432L)
}
\arguments{
  \item{source}{binary connection or character (which is interpreted as
    file name) specifying the source}
  \item{max.line}{maximum length of one line (in byets) - determines the
    size of the read buffer, default is 64kb}
  \item{reader}{reader object as returned by \code{chunk.reader}}
  \item{max.size}{maximum size of the chunk (in bytes), default is 32Mb}
}
\details{
  \code{chunk.reader} is essentially a filter that converts binary
  connection into chunks that can be subsequently parsed into data while
  preserving the integrity of input lines. \code{read.chunk} is used to
  read the actual chunks. The implementation is very thin to prevert
  copying of large vectors for best efficiency.
}
\value{
  \code{chunk.reader} returns an object that can be used by
  \code{read.chunk}. If \code{source} is a string, it is equivalent to
  calling \code{chunk.reader(file(source, "rb"), ...)}.

  \code{read.chunk} returns a raw vector holding the next chunk. It is
  deliberate that \code{read.chunk} does NOT return a character vector
  sicne that would reasult in a high performance penalty. Please use the
  approriate parser to convert the chunk into data, see
  \code{\link{mstrsplit}}.
}
\author{
Simon Urbanek
}
%\examples{
%}
\keyword{manip}
