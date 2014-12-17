\name{.default.formatter}
\alias{.default.formatter}
\title{
  Default formatter, coorisponding to the as.output functions
}
\description{
  This function provides the default formatter for the
  iotools package; it assumes that the key is
  seperated from the rest of the row by a tab character,
  and the elements of the row are seperated by the pipe
  ("|") character. Vector and matrix objects returned from
  the output via \code{\link{as.output}}.
}
\usage{
.default.formatter(x)
}
\arguments{
  \item{x}{character vector (each element is treated as a row) or a raw
    vector (LF characters \code{'\n'} separate rows) to split}
}
\value{
  Either a character matrix with a row for each element in the input, or
  a character vector with an element for each element in the input. The
  latter occurs when only one column (not counting the key) is detected
  in the input. The keys are stored as rownames or names, respectively.
}
%\references{
%}
\author{
  Simon Urbanek
}
\seealso{
\code{\link{mstrsplit}}
}
\examples{
  c <- c("A\tB|C|D", "A\tB|B|B", "B\tA|C|E")
  .default.formatter(c)
  
  c <- c("A\tD", "A\tB", "B\tA")
  .default.formatter(c)
}
\keyword{manip}
