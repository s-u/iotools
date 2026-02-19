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
chunk.apply(input, FUN, ..., CH.MERGE = rbind, CH.MAX.SIZE = 33554432,
            CH.PARALLEL=1L, CH.SEQUENTIAL=TRUE, CH.BINARY=FALSE,
            CH.INITIAL=NULL)

chunk.tapply(input, FUN, ..., sep, CH.MERGE = rbind, CH.MAX.SIZE = 33554432)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{input}{Either a chunk reader or a file name or connection that
    will be used to create a chunk reader}
  \item{FUN}{Function to apply to each chunk}
  \item{\dots}{Additional parameters passed to \code{FUN}}
  \item{sep}{singe character string. For \code{tapply}, gives
    separator for the key over which to apply. Each line is split at the
    first separator, and the preceding value is treated as the key over
    which to apply the function. If the \code{input} is a chunk reader,
    then this value is ignored (can be missing) and the key separator of
    the chunk reader is always used, otherwise defaults to \code{"\t"}
    and the corresponding chunk reader with that key separator is
    created internally.}
  \item{CH.MERGE}{Function to call to merge results from all
    chunks. Common values are \code{list} to get \code{lapply}-like
    behavior, \code{rbind} for table-like output or \code{c} for a long
    vector.}
  \item{CH.MAX.SIZE}{maximal size of each chunk in bytes}
  \item{CH.PARALLEL}{the number of parallel processes to use in the
    calculation (unix only).}
  \item{CH.SEQUENTIAL}{logical, only relevant for parallel
    processing. If \code{TRUE} then the chunks are guaranteed to be
    processed in sequential order. If \code{FALSE} then the chunks may
    be processed in any order to gain better performance.}
  \item{CH.BINARY}{logical, if \code{TRUE} then \code{CH.MERGE} is a
    binary function used to update the result object for each chunk,
    effectively acting like the \code{Reduce} function. If \code{FALSE}
    then the results from all chunks are accumulated first and then
    \code{CH.MERGE} is called with all chunks as arguments. See below
    for performance considerations.}
  \item{CH.INITIAL}{Function which will be applied to the first chunk if
    \code{CH.BINARY=TRUE}. If \code{NULL} then \code{CH.MERGE(NULL,
      chunk)} is called instead.}
}
\note{
  The input to \code{FUN} is the raw chunk, so typically it is
  advisable to use \code{\link{mstrsplit}} or similar function as the
  first step in \code{FUN}.
}
\value{
  The result of calling \code{CH.MERGE} on all chunk results as
  arguments (\code{CH.BINARY=FALSE}) or result of the last call to
  binary \code{CH.MERGE}.
}
\details{
  Due to the fact that chunk-wise processing is typically used when the
  input data is too large to fit in memory, there are additional
  considerations depending on whether the results after applying
  \code{FUN} are itself large or not. If they are not, then the apporach
  of accumulating them and then applying \code{CH.MERGE} on all results
  at once is typically the most efficient and it is what
  \code{CH.BINARY=FALSE} will do.

  However, in some situations where the result are resonably big or
  the number of chunks is very high, it may be more efficient to update
  a sort of state based on each arriving chunk instead of collecting all
  results. This can be achieved by setting \code{CH.BINARY=TRUE} in which
  case the process is equivalent to:
  \preformatted{res <- CH.INITIAL(FUN(chunk1))
res <- CH.MERGE(res, FUN(chunk2))
res <- CH.MERGE(res, FUN(chunk3))
...
res}

  If \code{CH.INITITAL} is \code{NULL} then the first line is
  \code{res <- CH.MERGE(NULL, FUN(chunk1))}.

  The parameter \code{CH.SEQUENTIAL} is only used if parallel
  processing is requested. It allows the system to process chunks out of
  order for performace reasons. If it is \code{TRUE} then the order of
  the chunks is respected, but merging can only proceed if the result of
  the next chunk is avaiable. With \code{CH.SEQUENTIAL=FALSE} the workers
  will continue processing further chunks as they become avaiable, not
  waiting for the results of the preceding calls. This is more
  efficient, but the order of the chunks in the result is not
  deterministic.

  Note that if parallel processing is required then all calls to
  \code{FUN} should be considered independent. However, \code{CH.MERGE}
  is always run in the current session and thus is allowed to have
  side-effects.
}
%\references{
%}
\author{
  Simon Urbanek
}
\note{
  The support for \code{CH.PARALLEL} is considered experimental and may
  change in the future.
}
%\seealso{
%}
\examples{
\dontrun{
## compute quantiles of the first variable for each chunk
## of at most 10kB size
chunk.apply("input.file.txt",
            function(o) {
              m = mstrsplit(o, type='numeric')
              quantile(m[,1], c(0.25, 0.5, 0.75))
            }, CH.MAX.SIZE=1e5)
}
}
\keyword{manip}
