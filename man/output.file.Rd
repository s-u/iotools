\name{output.file}
\alias{output.file}
\title{
  Write an R object to a file as a character string
}
\description{
  Writes any R object to a file or connection using an output
  formatter. Useful for pairing with the \code{input.file}
  function.
}
\usage{
  output.file(x, file, formatter.output = NULL)
}
\arguments{
  \item{x}{R object to write to the file}
  \item{file}{the input filename as a character string or a connection object
  open for writting.}
  \item{formatter.output}{a function for formatting the output. Using null
  will attempt to find the appropriate method given the class of the input
  \code{x}.}
}
\value{
  invisibly returns the input to \code{file}.
}
\author{
  Taylor Arnold and Simon Urbanek
}
\keyword{manip}
