\name{which.min.key}
\alias{which.min.key}
\title{
  Determine the next key in bytewise order
}
\description{
  \code{which.min.key} takes either a character vector or a list of
  strings and returns the location of the element that is
  lexicographically (using bytewise comparison) the first. In a sense
  it is \code{which.min} for strings. In addition, it supports prefix
  comparisons using a key delimiter (see below).
}
\usage{
which.min.key(keys, sep = "|")
}
\arguments{
  \item{keys}{character vector or a list of strings to use as input}
  \item{sep}{string specifying the key delimiter. Only the first
  character is used. Can be \code{""} if the entire string is to be
  treated as a key.}
}
\details{
  \code{which.min.key} considers the prefix of each element in
  \code{keys} up to the delimiter specified by \code{sep}. It returns
  the index of the element which is lexicographically first among all
  the elements, using bytewise comparison (i.e. the locale is not used
  and multi-byte characters are not considered as one character).
  
  If \code{keys} is a character vector then \code{NA} elements are
  treated as non-existent and will never be picked.

  If \code{keys} is a list then only string elements of length > 0 are
  eligible and \code{NA}s are not treated specially (hence they will
  be sorted in just like the \code{"NA"} string).
}
\value{
  scalar integer denoting the index of the lexicographically first
  element. In case of a tie the lowest index is returned. If there are
  no eligible elements in \code{keys} then a zero-length integer vector
  is returned.
}
%\references{
%}
\author{
  Simon Urbanek
}
\seealso{
\code{\link{which.min}}
}
\examples{
which.min.key(c("g","a","b",NA,"z","a"))
which.min.key(c("g","a|z","b",NA,"z|0","a"))
which.min.key(c("g","a|z","b",NA,"z|0","a"), "")
which.min.key(list("X",1,NULL,"F","Z"))
which.min.key(as.character(c(NA, NA)))
which.min.key(NA_character_)
which.min.key(list())
}
\keyword{manip}
