\name{readAsRaw}
\alias{readAsRaw}
\title{
  Read binary data in as raw
}
\description{
  \code{readAsRaw} takes a connection or file name and reads it into
  a raw type.
}
\usage{
readAsRaw(con, n, nmax, fileEncoding="")
}
\arguments{
  \item{con}{A connection object or a character string naming a file
    from which to save the output.}
  \item{n}{Expected number of bytes to read. Set to \code{65536L} by
    default when \code{con} is a connection, and set to the file size
    by default when \code{con} is a character string.}
  \item{nmax}{Maximum number of bytes to read; missing of \code{Inf}
    to read in the entire connection.}
  \item{fileEncoding}{When \code{con} is a connection, the file encoding
    to use to open the connection.}
}
\value{
  \code{readAsRaw} returns a \code{raw} type which can then be consumed
  by functions like \code{mstrsplit} and \code{dstrsplit}.
}
\author{
  Taylor Arnold
}
\examples{
  mm <- model.matrix(~., iris)
  f <- file("iris_mm.io", "wb")
  writeBin(as.output(mm), f)
  close(f)
  m <- mstrsplit(readAsRaw("iris_mm.io"), type="numeric", nsep="\t")
  head(mm)
  head(m)
  unlink("iris_mm.io")
}
