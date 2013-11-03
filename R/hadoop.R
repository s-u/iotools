hpath <- function(path) structure(path, class="HDFSpath")



hinput <- function(path, formatter=function(x) { y <- mstrsplit(x, '|', '\t'); if (ncol(y) == 1L) y[,1] else y })
  structure(path, class=c("hinput", "HDFSpath"), formatter=formatter)

hmr <- function(input, output, map=identity, reduce=identity, job.name, aux, formatter, packages=loadedNamespaces(), reducers) {
  .rn <- function(n) paste(sprintf("%04x", as.integer(runif(n, 0, 65536))), collapse='')
  if (missing(output)) output <- hpath(sprintf("/tmp/io-hmr-temp-%d-%s", Sys.getpid(), .rn(4)))
  if (missing(job.name)) job.name <- sprintf("RCloud:iotools:hmr-%s", .rn(2))
  if (!inherits(input, "HDFSpath")) stop("Sorry, you have to have the input in HDFS for now")
  if (missing(formatter) && inherits(input, "hinput")) formatter <- attr(input, "formatter")
  if (missing(formatter)) formatter <- mstrsplit
  hh <- Sys.getenv("HADOOP_HOME")
  if (!nzchar(hh)) hh <- Sys.getenv("HADOOP_PREFIX")
  if (!nzchar(hh)) hh <- "/usr/lib/hadoop"
  hcmd <- file.path(hh, "bin", "hadoop")
  if (!file.exists(hcmd)) stop("Cannot find working Hadoop home. Set HADOOP_PREFIX if in doubt.")
  sj <- Sys.glob(file.path(hh, "contrib", "streaming", "*.jar"))
  if (!length(sj)) stop("Cannot find streaming JAR - it should be in <HADOOP_PREFIX>/contrib/streaming")
  e <- new.env(parent=emptyenv())
  if (!missing(aux)) {
    if (is.list(aux)) for (n in names(aux)) e[[n]] <- aux[[n]]
    else if (is.character(aux)) for (n in aux) e[[n]] <- get(n) else stop("invalid aux")
  }
  e$formatter <- formatter
  e$map <- map
  e$reduce <- reduce
  e$load.packages <- packages
  f <- tempfile("hmr-stream-dir")
  dir.create(f,, TRUE, "0700")
  owd <- getwd()
  on.exit(setwd(owd))
  setwd(f)
  save(list=ls(envir=e, all.names=TRUE), envir=e, file="stream.RData")
  map.cmd <- if (identical(map, identity)) "" else "-mapper \"R --slave --vanilla -e 'iotools:::run.map()'\""
  reduce.cmd <- if (identical(reduce, identity)) "" else "-reducer \"R --slave --vanilla -e 'iotools:::run.reduce()'\""
  extraD <- if (missing(reducers)) "" else paste0("-D mapred.reduce.tasks=", as.integer(reducers))
  system(paste(
               shQuote(hcmd), "jar", shQuote(sj),
               "-D", "mapreduce.reduce.input.limit=-1",
               "-D", shQuote(paste0("mapred.job.name=", job.name)), extraD,
               "-input", shQuote(input),
               "-output", shQuote(output),
               "-file", "stream.RData",
               map.cmd, reduce.cmd))
  output
}
