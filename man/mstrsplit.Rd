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
mstrsplit(x, sep = "|", nsep = NA, line = 1L)
}
\arguments{
  \item{x}{character vector (each element is treated as a row) or a raw
    vector (LF characters \code{'\n'} separate rows) to split}
  \item{sep}{single character: field (column) separator}
  \item{nsep}{row name separator (single character) or \code{NA} if no
    row names are included}
  \item{line}{line number of the first line - used to identify lines in
    error reports, not used for actual processing}
}
\details{
  If the input is a raw vector, then it is interpreted as ASCII content
  with LF (\code{'\n'}) characters separating lines. If the input is a
  character vector then each element is treated as a line.
  
  If \code{nsep} is specified then all characters up to (but excluding)
  the occurrence of \code{nsep} are treated as the row name. The
  remaining characters are split using the \code{sep} character into
  fields (columns). The first line of the input determines the number of
  columns. \code{mstrsplit} will fail with an error if any subsequent
  line contains more columns. Subsequent lines can contain fewer columns
  in which case they are set to \code{NA}.

  The processing is geared thowards efficiency - no string re-coding is
  performed and raw input vector is processed directly, avoiding the
  creation of intermediate string representations.
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
}
\keyword{manip}
