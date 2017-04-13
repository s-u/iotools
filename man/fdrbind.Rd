\name{fdrbind}
\alias{fdrbind}
\title{
  Fast row-binding of lists and data frames
}
\description{
 \code{fdrbind} lakes a list of data frames or lists and merges them
 together by rows very much like \code{rbind} does for its
 arguments. But unlike \code{rbind} it specializes on data frames and
 lists of columns only and performs the merge entriley at C leve which
 allows it to be much faster than \code{rbind} at the cost of
 generality.
}
\usage{
fdrbind(list)
}
\arguments{
  \item{list}{lists of parts that can be either data frames or lists}
}
\details{
  All parts are expected to have the same number of columns in the
  same order. No column name matching is performed, they are merged by
  position. Also the same column in each part has to be of the same
  type, no coersion is performed at this point. The first part
  determines the column names, if any. If the parts contain data
  frames, their rownames are ignored, only the contents are merged.
  Attributes are not copied, which is intentional. Probaby the most
  common implocation is that ff you use factors, they must have all
  the same levels, otherwise you have to convert factor columns to
  strings first.
}
\value{
  The merged data frame.
}
%\references{
%}
\author{
Simon Urbanek
}
\seealso{
\code{\link{rbind}}
}
%\examples{
%}
\keyword{manip}
