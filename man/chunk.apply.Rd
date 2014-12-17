\name{chunk.apply}
\alias{chunk.apply}
\alias{chunk.tapply}
\title{
  Process input by applying a function to each chunk
}
\description{
  \code{chunk.apply} processes input in chunks and applies \code{FUN}
  to each chunk, collecting the results.
}
\usage{
chunk.apply(input, FUN, ..., CH.MERGE = rbind, CH.MAX.SIZE = 33554432)

chunk.tapply(input, FUN, ..., sep = "\t", CH.MERGE = rbind, CH.MAX.SIZE = 33554432)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{input}{Either a chunk reader or a file name or connection that
  will be used to create a chunk reader}
  \item{FUN}{Function to apply to each chunk}
  \item{\dots}{Additional parameters passed to \code{FUN}}
  \item{sep}{for tapply, gives separator for the key over which to apply. Each line
  is split at the first separator, and the value is treated as the key over which
  to apply the function over.}
  \item{CH.MERGE}{Function to call to merge results from all
  chunks. Common values are \code{list} to get \code{lapply}-like
  behavior, \code{rbind} for table-like output or \code{c} for a long
  vector.}
  \item{CH.MAX.SIZE}{maximal size of each chunk in bytes}
}
\note{
  The input to \code{FUN} is the raw chunk, so typically it is
  advisabe to use \code{\link{mstrsplit}} or similar function as the
  first setep in \code{FUN}.
}
\value{
  The result of calling \code{CH.MERGE} on all chunk results.
}
%\references{
%}
\author{
 Simon Urbanek
}
%\note{
%}
%\seealso{
%}
\examples{
\dontrun{
## compute quantiles of the first variable for each chunk
## of at most 10kB size
chunk.apply("input.file.txt",
            function(o) {
              m = mstrsplit(o)
              quantile(as.numeric(m[,1]), c(0.25, 0.5, 0.75))
            }, CH.MAX.SIZE=1e5)
}
}
\keyword{manip}
