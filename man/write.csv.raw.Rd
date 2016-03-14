\name{write.csv.raw}
\alias{write.csv.raw}
\alias{write.table.raw}
\title{
  Fast data output to disk
}
\description{
  A fast replacement of \code{write.csv} and \code{write.table} which
  saves the data as a raw vector rather than a character one.
}
\usage{
write.csv.raw(x, file = "", append = FALSE, sep = ",", nsep="\t",
              col.names = !is.null(colnames(x)), fileEncoding = "")

write.table.raw(x, file = "", sep = " ", ...)
}
\arguments{
  \item{x}{object which is to be saved.}
  \item{file}{A connection object or a character string naming a file
    from which to save the output.}
  \item{append}{logical. Only used when file is a character string.}
  \item{sep}{field (column) separator.}
  \item{nsep}{index name separator (single character) or \code{NA} if no
    index names are included}
  \item{col.names}{logical. Should a raw of column names be writen.}
  \item{fileEncoding}{character string: if non-empty declares the encoding to
          be used on a file.}
  \item{...}{additional parameters to pass to \code{write.table.raw}.}
}
\details{
  See \code{\link{as.output}} for the details of how various data types are
  converted to raw vectors (or character vectors when raw is not available).
}
\author{
  Taylor Arnold and Simon Urbanek
}
\keyword{manip}
