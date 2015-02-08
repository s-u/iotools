\name{dstrsplit}
\alias{dstrsplit}
\alias{rowindex}
\title{
  Split binary or character input into a dataframe
}
\description{
  \code{dstrsplit} takes raw or character vector and splits it
  into a dataframe according to the separators.
}
\usage{
dstrsplit(x, col_types, sep="|", nsep=NA, strict=TRUE, skip=0L, nrows=-1L)
}
\arguments{
  \item{x}{character vector (each element is treated as a row) or a
    raw vector (newlines separate rows)}
  \item{col_types}{required character vector or a list. A vector of
    classes to be assumed for the output dataframe. If it is a list,
    \code{class(x)[1]} will be used to determine the class of the
    contained element. It will not be recycled, and must
    be at least as long as the longest row if \code{strict} is \code{TRUE}.

    Possible values are \code{"NULL"} (when the column is skipped) one of
    the six atomic vector types (\code{'character'}, \code{'numeric'},
    \code{'logical'}, \code{'integer'},  \code{'complex'}, \code{'raw'})
     or \code{POSIXct}.
    'POSIXct' will parse date format in the form "YYYY-MM-DD hh:mm:ss.sss"
    assuming GMT time zone. The separators between digits can be any
    non-digit characters and only the date part is mandatory. See also
    \code{fasttime::asPOSIXct} for details.
  }
  \item{sep}{single character: field (column) separator. Set to \code{NA}
    for no seperator; in other words, a single column.}
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
  \code{dstrsplit} returns a data.frame with as many rows as
  they are lines in the input and as many columns as there are
  non-NULL values in \code{col_types}, plus an additional column if
  \code{nsep} is specified. The colnames (other than the row index)
  are set to 'V' concatenated with the column number unless
  \code{col_types} is a named vector in which case the names are
  inherited.
}
\author{
  Taylor Arnold and Simon Urbanek
}
\examples{
input = c("apple\t2|2.7|horse|0d|1|2015-02-05 20:22:57",
          "pear\t7|3e3|bear|e4|1+3i|2015-02-05",
          "pear\te|1.8|bat|77|4.2i|2001-02-05")
z = dstrsplit(x = input,
      col_types = c("integer", "numeric", "character","raw","complex","POSIXct"),
      sep="|", nsep="\t")
lapply(z,class)
z

# Ignoring the third column:
z = dstrsplit(x = input,
      col_types = c("integer", "numeric", "character","raw","complex","POSIXct"),
      sep="|", nsep="\t")
z
}
\keyword{manip}
