\name{readAsRaw}
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
  \item{con}{}
  \item{n}{}
  \item{nmax}{}
  \item{fileEncoding}{}
}
\details{
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
