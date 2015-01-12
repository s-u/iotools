\name{as.output}
\alias{as.output}
\alias{as.output.default}
\alias{as.output.data.frame}
\alias{as.output.list}
\alias{as.output.matrix}
\alias{as.output.table}
\title{
  Character Output
}
\description{
  Create objects of class \code{output}.
}
\usage{
  as.output(x, ...)
}
\arguments{
  \item{x}{object to be converted to an instance of \code{output}.}
  \item{...}{optional arguments to be passed to implementing methods
  of \code{as.output}}
}
\details{
  \code{as.output} is generic, and methods can be written to support
  new classes. The output is meant to be an character vector suitable for
  writing to the disk or sending over a connection.
}
\value{
  \code{as.output.default} outputs a character vector by default, though
  other formats (such as a raw vector) may be returned by other methods.
}
\author{
Simon Urbanek
}
\examples{
  m = matrix(sample(letters), ncol=2)
  as.output(m)

  df = data.frame(a = sample(letters), b = runif(26), c = sample(state.abb,26))
  as.output(df)
}
\keyword{manip}
