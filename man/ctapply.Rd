\name{ctapply}
\alias{ctapply}
\title{
Fast tapply() replacement functions
}
\description{
  \code{ctapply} is a fast replacement of \code{tapply} that assumes
  contiguous input, i.e. unique values in the index are never speparated
  by any other values. This avoids an expensive \code{split} step since
  both value and the index chungs can be created on the fly. It also
  cuts a few corners to allow very efficient copying of values. This
  makes it many orders of magnitude faster than the classical
  \code{lapply(split(), ...)} implementation.
}
\usage{
ctapply(X, INDEX, FUN, ..., MERGE=c)
}
\arguments{
  \item{X}{an atomic object, typically a vector}
  \item{INDEX}{numeric or character vector of the same length as \code{X}}
  \item{FUN}{the function to be applied}
  \item{...}{additional arguments to \code{FUN}. They are passed as-is,
    i.e., without replication or recycling}
  \item{MERGE}{function to merge the resulting vector or \code{NULL} if
    the arguments to such a functiona re to be returned instead}
}
\details{
  Note that \code{ctapply} supports either integer, real or character
  vectors as indices (note that factors are integer vectors and thus
  supported, but you do not need to convert character vectors). Unlike
  \code{tapply} it does not take a list of factors - if you want to use
  a cross-product of factors, create the product first, e.g. using
  \code{paste(i1, i2, i3, sep='\01')} or multiplication - whetever
  method is convenient for the input types.

  \code{ctapply} requires the \code{INDEX} to contiguous. One (slow) way
  to achieve that is to use \code{\link{sort}} or \code{\link{order}}.

  \code{ctapply} also supports \code{X} to be a matrix in which case it
  is split row-wise based on \code{INDEX}. The number of rows must match
  the length of \code{INDEX}. Note that the indexed matrices behave as
  if \code{drop=FALSE} was used and curretnly \code{dimnames} are only
  honored if rownames are present.
}
%\value{
%}
%\references{
%}
\author{
Simon Urbanek
}
\note{
  This function has been moved to the \code{fastmatch} package!
}
%% ~Make other sections like Warning with \section{Warning }{....} ~
\seealso{
\code{\link{tapply}}
}
\examples{
i = rnorm(4e6)
names(i) = as.integer(rnorm(1e6))
i = i[order(names(i))]
system.time(tapply(i, names(i), sum))
system.time(ctapply(i, names(i), sum))

## ctapply() also works on matrices (unlike tapply)
m=matrix(c("A","A","B","B","B","C","A","B","C","D","E","F","","X","X","Y","Y","Z"),,3)
ctapply(m, m[,1], identity, MERGE=list)
ctapply(m, m[,1], identity, MERGE=rbind)
m2=m[,-1]
rownames(m2)=m[,1]
colnames(m2) = c("V1","V2")
ctapply(m2, rownames(m2), identity, MERGE=list)
ctapply(m2, rownames(m2), identity, MERGE=rbind)
}
\keyword{manip}
