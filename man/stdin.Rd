\name{readSTDIN}
\alias{readSTDIN}
\alias{writeBIN}
\title{
  Read from stdin, write to stdout
}
\description{
  \code{readSTDIN} reads lines from \code{stdin}, and
  \code{writeBIN} writes binary connection to \code{stdout}.
}
\usage{
readSTDIN(n = 10000L)

writeBIN(what, flush=TRUE)
}
\arguments{
  \item{n}{integer, number of lines to read at most}
  \item{what}{raw object to output to stdout}
  \item{flush}{whether binary output should be flushed}
}  
%\details{
%}
\value{
  character vector of at most length \code{n}. If it is shorter than
  \code{n} then it signifies end of input.
}
\note{
  This is roughly equivalent to calling \code{readLines()} on
  \code{file("stdin", "r")} and is retained for compatibility for some
  experimental code.
}
  \author{
  Simon Urbanek
}
%\examples{
%}
\keyword{manip}
