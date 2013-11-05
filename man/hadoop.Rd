\name{hadoop}
\alias{hadoop}
\alias{hmr}
\alias{hinput}
\alias{hpath}
\title{
  Experimental Hadoop chunk-processing code
}
\description{
  \code{readSTDIN} reads lines from \code{stdin}
}
\usage{
hmr(input, output, map = identity, reduce = identity, job.name,
    aux, formatter, packages = loadedNamespaces())
hpath(path)
hinput(path, formatter = function(x) {
    y <- mstrsplit(x, "|", "\t")
    if (ncol(y) == 1L)
        y[, 1]
    else y
})
}
\arguments{
  \item{input}{input data - see details}
  \item{output}{output path (optional)}
  \item{map}{chunk compute function (map is a misnomer)}
  \item{reduce}{chunk combine function}
  \item{job.name}{name of the job to pass to Hadoop}
  \item{aux}{either a character vector of symbols names or a named list
    of values to push to the compute nodes}
  \item{formatter}{formatter to use. It is optional in \code{hmr} if the
    input source already contains a formatter definition}
  \item{packages}{character vector of package names to attach on the
    compute nodes}
  \item{reducers}{optional integer specifying the number of parallel
    jobs in the combine step. It is a hint in the sense that any number
    greater than one implies independence of the chunks in the combine
    step. Default is to not assume independence.}
  \item{path}{HDFS path}
}
\details{
  \code{hmr} creates and runs a Hadoop job to perform chunkwise compute
  + combine. The input is read using \code{\link{chunk.reader}},
  processed using the \code{formatter} function and passed to the
  \code{map} function. The result is converted using \code{as.output}
  before going back to Hadoop. The chunkwise results are combined using
  the \code{reduce} function - the flow is the same as in the \code{map}
  case. Then result is returned as HDFS path. Either \code{map} or
  \code{reduce} can be \code{identity} (the default).

  \code{hpath} tags a string as HDFS path. The sole purpose here is to
  distiguish local and HDFS paths.

  \code{hinput} creates a subclass of \code{HDFSpath} which also
  contains the definition of the formatter for that path. The default
  formatter honors default Hadoop settings of \code{'\t'} as the
  key/value separator and \code{'|'} as the field separator.
}
\value{
  \code{hmr} returns the HDFS path to the result when finished.

  \code{hpath} returns a character vector of class \code{"HDFSpath"}

  \code{hinput} returns a subclass \code{"hinput"} of \code{"HDFSpath"}
  containing the additional \code{"formatter"} attribute.
}
\note{
  Requires properly installed Hadoop client. The installation must
  either be in \code{/usr/lib/hadoop} or one of \code{HADOOP_HOME},
  \code{HADOOP_PREFIX} environment variables must be set accordingly.
}
\author{
  Simon Urbanek
}
\examples{
\dontrun{
## map points to ZIP codes and count the number of points per ZIP
## uses Tiger/LINE 2010 census data shapefiles
## we can use ctapply becasue Hadoop guarantees contiguous input

## require(fastshp); require(tl2010)
r <- hmr(
  hinput("/data/points"),
  map = function(x)
    table(zcta2010.db()[
       inside(zcta2010.shp(), x[,4], x[,5]), 1]),
  reduce = function(x) ctapply(as.numeric(x), names(x), sum))
}
}
\keyword{manip}
