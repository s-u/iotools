\name{mstrsplit}
\alias{mstrsplit}
\title{
  Split binary or character input into a matrix
}
\description{
  \code{mstrsplit} takes either raw or character vector and splits it
  into a character matrix according to the separators.
}
\usage{
mstrsplit(x, sep="|", nsep=NA, strict=TRUE, ncol = NA,
          type=c("character", "numeric", "logical", "integer",  "complex", "raw"),
          skip=0L, nrows=-1L)
}
\arguments{
  \item{x}{character vector (each element is treated as a row) or a raw
    vector (LF characters \code{'\n'} separate rows) to split}
  \item{sep}{single character: field (column) separator. Set to \code{NA}
    for no seperator; in other words, a single column.}
  \item{nsep}{row name separator (single character) or \code{NA} if no
    row names are included}
  \item{strict}{logical, if \code{FALSE} then \code{mstrsplit} will not
    fail on parsing errors, otherwise input not matching the format
    (e.g. more columns than expected) will cause an error.}
  \item{ncol}{number of columns to expect. If \code{NA} then the number
    of columns is guessed from the first line.}
  \item{type}{a character string representing one of the 6 atomic types:
    \code{'character'}, \code{'numeric'}, \code{'logical'}, \code{'integer'},
    \code{'complex'}, or \code{'raw'}. The output matrix will use this as its
    storage mode and the input will be parsed directly into this format
    without using intermediate strings.}
  \item{skip}{integer: the number of lines of the data file to skip before
    parsing records.}
  \item{nrows}{integer: the maximum number of rows to read in.  Negative and
    other invalid values are ignored, and indiate that the entire input
    should be processed.}
}
\details{
  If the input is a raw vector, then it is interpreted as ASCII/UTF-8 content
  with LF (\code{'\n'}) characters separating lines. If the input is a
  character vector then each element is treated as a line.

  If \code{nsep} is specified then all characters up to (but excluding)
  the occurrence of \code{nsep} are treated as the row name. The
  remaining characters are split using the \code{sep} character into
  fields (columns). If \code{ncol} is \code{NA} then the first line of
  the input determines the number of columns. \code{mstrsplit} will fail
  with an error if any line contains more columns then expected unless
  \code{strict} is \code{FALSE}. Excessive columns are ignored in that
  case. Lines may contain fewer columns in which case they are set to
  \code{NA}.

  The processing is geared towards efficiency - no string re-coding is
  performed and raw input vector is processed directly, avoiding the
  creation of intermediate string representations.

  Note that it is legal to use the same separator for \code{sep} and
  \code{nsep} in which case the first field is treated as a row name and
  subsequent fields as data columns.
}
\value{
  A matrix with as many rows as they are lines in the input and
  as many columns as there are fields in the first line. The
  storage mode of the matrix will be determined by the input to
  \code{type}.
}
\author{
  Simon Urbanek
}
\examples{
  c <- c("A\tB|C|D", "A\tB|B|B", "B\tA|C|E")
  m <- mstrsplit(gsub("\t","|",c))
  dim(m)
  m
  m <- mstrsplit(c,, "\t")
  rownames(m)
  m

  ## use raw vectors instead
  r <- charToRaw(paste(c, collapse="\n"))
  mstrsplit(r)
  mstrsplit(r, nsep="\t")
}
\keyword{manip}
