\name{dstrsplit}
\alias{dstrsplit}
\alias{rowindex}
\title{
  Split ASCII input into a dataframe
}
\description{
  \code{dstrsplit} takes a character vector and splits it
  into a dataframe according to the separators.
}
\usage{
dstrsplit(x, col_types, sep="|", nsep=NA, strict=TRUE)

}
\arguments{
  \item{x}{character vector (each element is treated as a row)}
  \item{col_types}{required character. A vector of classes to be
  assumed for the output dataframe. Will not be recycled, and must
  be at least as long as the longest row if \code{strict} is \code{TRUE}.

  Possible values are 'integer', 'numeric', 'character', and NA. The
  latter indicates that the column should be skipped in the output.}
  \item{sep}{single character: field (column) separator}
  \item{nsep}{index name separator (single character) or \code{NA} if no
    index names are included}
  \item{strict}{logical, if \code{FALSE} then \code{mstrsplit} will not
    fail on parsing errors, otherwise input not matching the format
    (e.g. more columns than expected) will cause an error.}
}
\details{
  If \code{nsep} is specified then all characters up to (but excluding)
  the occurrence of \code{nsep} are treated as the index name. The
  remaining characters are split using the \code{sep} character into
  fields (columns). \code{dstrsplit} will fail with an error if any
  line contains more columns then expected unless \code{strict} is
  \code{FALSE}. Excessive columns are ignored in that case. Lines may
  contain fewer columns in which case they are set to \code{NA}.

  Note that it is legal to use the same separator for \code{sep} and
  \code{nsep} in which case the first field is treated as a row name and
  subsequent fields as data columns.

  If \code{nsep} is specified, the output of \code{dstrsplit} contains
  an extra column called 'rowindex' containing the row index. This is
  used instead of the rownames to allow for duplicated indicies (which
  are checked for and not allowed in a dataframe, unlike the case with
  a matrix).
}
\value{
  \code{dstrsplit} returns a Dataframe with as many rows as
  they are lines in the input and as many columns as there are
  non-NA values in \code{col_types}, plus an additiona column if
  \code{nsep} is specified. The colnames (other than the row index)
  are set to 'V' concatenated with the column number; rownames are
  a sequence of integers from 1 to the number of rows.
}
\author{
  Taylor Arnold and Simon Urbanek
}
\examples{
input = c("apple\t2|2.7|horse|3", "pear\t7|3.4|bear|-3", "pear\t2|1.8|bat|1")
z = dstrsplit(x = input, col_types = c("integer", "numeric", "character", "integer"),
              sep="|", nsep="\t")
z

# Ignoring the third column:
z = dstrsplit(x = input, col_types = c("integer", "numeric", NA, "integer"),
              sep="|", nsep="\t")
z
}
\keyword{manip}
