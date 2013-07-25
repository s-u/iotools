\name{set.}
\alias{set.tempdir}
\title{
  Change R's temporary directory
}
\description{
  \code{set.tempdir} changes the directury returned by
  \code{\link{tempdir}()}. It should only be used after \code{fork}ing
  (in particular when switching users) so that multiple processes don't
  share the temporary directory.
}
\usage{
set.tempdir(path)
}
\arguments{
  \item{path}{string, path to use as the temporary directory}
}
\details{
  Normally \code{tempdir()} is created and initialized on R startup and
  should not be changed. However, when using \code{fork()} the child
  process inherits the same directory which may not be desirable, in
  particular when the child wants to use \code{\link{set.user}} to
  change the owner. In such cases \code{set.tempdir} can be used to
  change the temporary directory that R will be using.

  \code{path} will be passed to \code{\link{path.expand}} first.
}
\value{
  The newly set path
}
%\references{
%}
\author{
  Simon Urbanek
}
\note{
  \code{set.tempdir} does \emph{not} check that the path is valid and/or
  writable. it is th eusere's responsibility to create teh path and then
  use \code{set.tempdir}, otherwise R may not function properly.
}
%\seealso{
%}
\examples{
\dontrun{
## create process-specific tempdir
dir.create(t <- paste(tempdir(), Sys.getpid(), sep='-'), FALSE, TRUE, "0700")
set.tempdir(t)
}
}
\keyword{IO}
