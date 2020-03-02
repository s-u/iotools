\name{as.output}
\alias{as.output}
\alias{as.output.default}
\alias{as.output.data.frame}
\alias{as.output.list}
\alias{as.output.matrix}
\alias{as.output.table}
\alias{as.output.raw}
\alias{iotools.stdout}
\alias{iotools.stderr}
\alias{iotools.fd}
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
    of \code{as.output}. Most methods support the following arguments:
    \code{sep} string, column/value separator, \code{nsep} string, key
    separator, \code{keys} either a logical (if \code{FALSE} names/row
    names are suppressed) or a character vector with overriding
    keys. The default for \code{keys} typically varies by class or is
    auto-detected (e.g., named vectors user names as keys,
    \code{data.frames} use row names if they are non-automatic
    etc.). All methods also support \code{con} argument which pushes
    the output into a connection instead of generating an output object
    - so \code{as.output(x, con=...)} is thus not a coersion but used
    only for its side-effect.
    Note that \code{con} also supports special values
    \code{iotools.stdout}, \code{iotools.stderr} and
    \code{iotools.fd(fd)} which write directly into the corresponding
    streams instead of using theconnection API.
  }
}
\details{
  \code{as.output} is generic, and methods can be written to support
  new classes. The output is meant to be a raw vector suitable for
  writing to the disk or sending over a connection.
}
\value{
  if \code{con} is set to a connection then the result is \code{NULL}
  and the method is used for its side-effect, otherwise the result is a
  raw vector.

  Side note: we cannot create a formal type of \code{output}, because
  \code{writeBin} does \code{is.vector()} check which doesn't dispatch
  and prevents anything with a class to be written.
}
\author{
Simon Urbanek
}
\examples{
  m = matrix(sample(letters), ncol=2)
  as.output(m)

  df = data.frame(a = sample(letters), b = runif(26), c = sample(state.abb,26))
  str(as.output(df))

  as.output(df, con=iotools.stdout)
}
\keyword{manip}
