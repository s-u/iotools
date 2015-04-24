\name{read.csv.raw}
\alias{read.csv.raw}
\alias{read.delim.raw}
\title{
  Fast data frame input
}
\description{
  A fast replacement of \code{read.csv} and \code{read.delim} which
  pre-loads the data as a raw vector and parses without constructing
  intermediate strings.
}
\usage{
read.csv.raw(file, header=TRUE, sep=",", skip=0L, fileEncoding="",
              colClasses, nrows = -1L, nsep = NA, strict=TRUE,
              nrowsClasses = 25L)

read.delim.raw(file, header=TRUE, sep="\t", ...)
}
\arguments{
  \item{file}{A connection object or a character string naming a file
    from which to read data.}
  \item{header}{logical. Does a header row exist for the data.}
  \item{sep}{single character: field (column) separator.}
  \item{skip}{integer. Number of lines to skip in the input, no including
    the header.}
  \item{fileEncoding}{The name of the encoding to be assumed. Only used when
    \code{con} is a character string naming a file.}
  \item{colClasses}{an optional character vector indicating the column
    types. A vector of classes to be assumed for the output dataframe.
    If it is a list, \code{class(x)[1]} will be used to determine the class
    of the contained element. It will not be recycled, and must
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
  \item{nrows}{integer: the maximum number of rows to read in.  Negative and
          other invalid values are ignored.}
  \item{nsep}{index name separator (single character) or \code{NA} if no
    index names are included}
  \item{strict}{logical, if \code{FALSE} then \code{dstrsplit} will not
    fail on parsing errors, otherwise input not matching the format
    (e.g. more columns than expected) will cause an error.}
  \item{nrowsClasses}{integer. Maximum number of rows of data to read to learn column
    types. Not used when \code{col_types} is supplied. }
  \item{...}{additional parameters to pass to \code{read.csv.raw}}
}
\details{
  See \code{\link{dstrsplit}} for the details of \code{nsep}, \code{sep},
  and \code{strict}.
}
\value{
  A data frame containing a representation of the data in the file.
}
\author{
  Taylor Arnold and Simon Urbanek
}
\keyword{manip}
