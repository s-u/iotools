\name{line.merge}
\alias{line.merge}
\title{
Merge multiple sources
}
\description{
Read lines for a collection of sources and merges the results to a single output.
}
\usage{
line.merge(sources, target, sep = "|", close = TRUE)
}
\arguments{
  \item{sources}{A list or vector of connections which need to be merged}
  \item{target}{A connection object or a character string giving the output
  of the merge. If a character string a new file connection will be created
  with the supplied file name.}
  \item{sep}{string specifying the key delimiter. Only the first character
          is used. Can be \code{""} if the entire string is to be treated as
          a key.}
  \item{close}{logical. Should the input to sources be closed by the function.}
}
\value{
  No explicit value is returned. The function is used purely for its side effects
  on the sources and target.
}
\author{
 Simon Urbanek
}
\keyword{ manip }
