\name{idstrsplit}
\alias{idstrsplit}
\title{
  Create an iterator for splitting binary or character input into a dataframe
}
\description{
  \code{idstrsplit} takes a binary connection or character vector (which is 
  interpreted as a file name) and splits it into a series of dataframes 
  according to the separator.
}
\usage{
idstrsplit(x, col_types, sep="|", nsep=NA, strict=TRUE, 
           max.line = 65536L, max.size = 33554432L)
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
  \item{max.line}{maximum length of one line (in byets) - determines the
    size of the read buffer, default is 64kb}
  \item{max.size}{maximum size of the chunk (in bytes), default is 32Mb}
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
  \code{idstrsplit} returns an iterator (closure). When \code{nextElem} is
  called on the iterator a data.frame is returned with as many rows as
  they are lines in the input and as many columns as there are
  non-NULL values in \code{col_types}, plus an additional column if
  \code{nsep} is specified. The colnames (other than the row index)
  are set to 'V' concatenated with the column number unless
  \code{col_types} is a named vector in which case the names are
  inherited.
}
\author{
  Michael Kane
}
\examples{
col_names <- names(iris)
write.csv(iris, file="iris.csv", row.names=FALSE)
it <- idstrsplit("iris.csv", col_types=c(rep("numeric", 4), "character"), 
                 sep=",")
# Get the elements
iris_read <- it$nextElem()[-1,]
# or with the iterators package
# nextElem(it)
names(iris_read) <- col_names
print(head(iris_read))
unlink("iris.csv")
}
\keyword{iterator}
