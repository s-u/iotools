\name{chunk.map}
\alias{chunk.map}
\title{
  Map a function over a file by chunks
}
\description{
  A wrapper around the core iotools functions to easily
  apply a function over chunks of a large file. Results can
  be either written to a file or returned as an internal list.
}
\usage{
chunk.map(input, output = NULL, formatter = .default.formatter,
          FUN, key.sep = NULL, max.line = 65536L,
          max.size = 33554432L, output.sep = ",", output.nsep = "\t",
          output.keys = FALSE, skip = 0L, ...)
}
\arguments{
  \item{input}{an input connection or character vector describing a local file.}
  \item{output}{an optional output connection or character vector describing a local file.
    If \code{NULL}, the results are returned internally as a list.}
  \item{formatter}{a function that takes raw input and produces the input given to \code{FUN}}
  \item{FUN}{a user provided function to map over the chunks. The result of FUN is either
    wrapper in a list item, when \code{output} is \code{NULL}, or written to the output file
    using \code{\link{as.output}}}
  \item{key.sep}{optional key separator given to \code{\link{chunk.reader}}}
  \item{max.line}{maximum number of lines given to \code{\link{chunk.reader}}}
  \item{max.size}{maximum size of a block as given to \code{\link{read.chunk}}}
  \item{output.sep}{single character giving the field separator in the output.}
  \item{output.nsep}{single character giving the key separator in the output.}
  \item{output.keys}{logical. Whether as.output should interpret row names as keys.}
  \item{skip}{integer giving the number of lines to strip off the input before reading. Useful
    when the input contains a row a column headers}
  \item{...}{additional parameters to pass to \code{FUN}}
}
\value{
  A list of results when \code{output} is \code{NULL}; otherwise no output is returned.
}
\author{
  Taylor Arnold
}
\keyword{manip}
