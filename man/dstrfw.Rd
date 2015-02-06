\name{dstrfw}
\alias{dstrfw}
\title{
  Split fixed width input into a dataframe
}
\description{
  \code{dstrfw} takes raw or character vector and splits it
  into a dataframe according to a vector of fixed widths.
}
\usage{
dstrfw(x, col_types, widths, nsep = NA, strict=TRUE, skip=0L, nrows=-1L)
}
\arguments{
  \item{x}{character vector (each element is treated as a row) or a
    raw vector (newlines separate rows)}
  \item{col_types}{required character vector or a list. A vector of
    classes to be assumed for the output dataframe. If it is a list,
    \code{class(x)[1]} will be used to determine the class of the
    contained element. It will not be recycled, and must
    be at least as long as the longest row if \code{strict} is \code{TRUE}.

    Possible values are 'integer', 'numeric', 'character', and NA. The
    latter indicates that the column should be skipped in the output.}
  \item{widths}{a vector of widths of the columns. Must be the same length
    as \code{col_types}.}
  \item{nsep}{index name separator (single character) or \code{NA} if no
    index names are included}
  \item{strict}{logical, if \code{FALSE} then \code{dstrsplit} will not
    fail on parsing errors, otherwise input not matching the format
    (e.g. more columns than expected) will cause an error.}
  \item{skip}{integer: the number of lines of the data file to skip before
            beginning to read data.}
  \item{nrows}{integer: the maximum number of rows to read in.  Negative and
          other invalid values are ignored.}
}
\details{
  If \code{nsep} is specified, the output of \code{dstrsplit} contains
  an extra column called 'rowindex' containing the row index. This is
  used instead of the rownames to allow for duplicated indicies (which
  are checked for and not allowed in a dataframe, unlike the case with
  a matrix).
}
\value{
  If \code{nsep} is specified then all characters up to (but excluding)
  the occurrence of \code{nsep} are treated as the index name. The
  remaining characters are split using the \code{widths} vector into
  fields (columns). \code{dstrfw} will fail with an error if any
  line does not contain enough characters to fill all expected columns,
  unless \code{strict} is \code{FALSE}. Excessive columns are ignored
  in that case. Lines may contain fewer columns (but not partial ones
  unless \code{strict} is \code{FALSE}) in which case they are set to
  \code{NA}.

  \code{dstrfw} returns a data.frame with as many rows as
  they are lines in the input and as many columns as there are
  non-NA values in \code{col_types}, plus an additional column if
  \code{nsep} is specified. The colnames (other than the row index)
  are set to 'V' concatenated with the column number unless
  \code{col_types} is a named vector in which case the names are
  inherited.
}
\author{
  Taylor Arnold and Simon Urbanek
}
\examples{
input = c("bear\t22.7horse+3", "pear\t 3.4mouse-3", "dogs\t14.8prime-8")
z = dstrfw(x = input, col_types = c("numeric", "character", "integer"),
              width=c(4L,5L,2L), nsep="\t")
z

# Now without row names (treat seperator as a 1 char width column with type NA)
z = dstrfw(x = input, col_types = c("character", NA, "numeric", "character", "integer"),
              width=c(4L,1L,4L,5L,2L))
z
}
\keyword{manip}
