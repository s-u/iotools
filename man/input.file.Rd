\name{input.file}
\alias{input.file}
\title{
  Load a file on the disk
}
\description{
  \code{input.file} efficently reads a file on the disk into R using
  a formatter function. The function may be \code{mstrsplit},
  \code{dstrsplit}, \code{dstrfw}, but can also be a user-defined
  function.
}
\usage{
input.file(file_name, formatter = mstrsplit, ...)
}
\arguments{
  \item{file_name}{the input filename as a character string}
  \item{formatter}{a function for formatting the input. \code{mstrsplit}
  is used by default.}
  \item{...}{other arguments passed to the formatter}
}
\value{
  the return type of the formatter function; by default a character matrix.
}
\author{
  Taylor Arnold and Simon Urbanek
}
\keyword{manip}
