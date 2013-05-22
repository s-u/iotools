\name{readSTDIN}
\alias{readSTDIN}
\title{
  Read from stdin
}
\description{
  \code{readSTDIN} reads lines from \code{stdin}
}
\usage{
readSTDIN(n = 10000L)
}
\arguments{
  \item{n}{integer, number of lines to read at most}
}
%\details{
%}
\value{
  character vector of at most length \code{n}. If it is shorter than
  \code{n} then it signifies end of input.
}
\author{
Simon Urbanek
}
%\examples{
%}
\keyword{manip}
