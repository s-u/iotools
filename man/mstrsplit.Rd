\name{mstrsplit}
\alias{mstrsplit}
\title{
  Split ASCII input into a matrix
}
\description{
  \code{mstrsplit} takes either raw or character vector and splits it
  into a character matrix according to the separators.
}
\usage{
mstrsplit(x, sep = "|", nsep = NA, line = 1L, quiet = FALSE, ncol = NA,
          type=c("character", "numeric"))
}
\arguments{
  \item{x}{character vector (each element is treated as a row) or a raw
    vector (LF characters \code{'\n'} separate rows) to split}
  \item{sep}{single character: field (column) separator}
  \item{nsep}{row name separator (single character) or \code{NA} if no
    row names are included}
  \item{line}{line number of the first line - used to identify lines in
    error reports, not used for actual processing}
  \item{quiet}{logical, if \code{TRUE} then \code{mstrsplit} will not
    fail on parsing errors, otherwise input not matching the format
    (e.g. more columns than expected) will cause an error.}
  \item{ncol}{number of columns to expect. If \code{NA} then the number
    of columns is guessed from the first line.}
  \item{type}{one of 'character' or 'numeric'; if the latter, the output
    will be converted to numeric values on the fly}
}
\details{
  If the input is a raw vector, then it is interpreted as ASCII content
  with LF (\code{'\n'}) characters separating lines. If the input is a
  character vector then each element is treated as a line.
  
  If \code{nsep} is specified then all characters up to (but excluding)
  the occurrence of \code{nsep} are treated as the row name. The
  remaining characters are split using the \code{sep} character into
  fields (columns). If \code{ncol} is \code{NA} then the first line of
  the input determines the number of columns. \code{mstrsplit} will fail
  with an error if any line contains more columns then expected unless
  \code{quiet} is \code{TRUE}. Excessive columns are ignored in that
  case. Lines may contain fewer columns in which case they are set to
  \code{NA}.

  The processing is geared thowards efficiency - no string re-coding is
  performed and raw input vector is processed directly, avoiding the
  creation of intermediate string representations.

  Note that it is legal to use the same separator for \code{sep} and
  \code{nsep} in which case the first field is treated as a row name and
  subsequent fields as data columns.
}
\value{
  Character matrix with as many rows as they are lines in the input and
  as many columns as there are fields in the first line.
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
