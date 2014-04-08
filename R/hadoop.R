hpath <- function(path) structure(path, class="HDFSpath")

.default.formatter <- function(x) {
  y <- mstrsplit(x, "|", "\t")
  if (ncol(y) == 1L) y[, 1] else y
}

# FIXME: we could use .default.formatter except that it's hidden - we should export it and document it
hinput <- function(path, formatter=function(x) { y <- mstrsplit(x, '|', '\t'); if (ncol(y) == 1L) y[,1] else y })
  structure(path, class=c("hinput", "HDFSpath"), formatter=formatter)

hmr <- function(input, output, map=identity, reduce=identity, job.name, aux, formatter, packages=loadedNamespaces(), reducers) {
  .rn <- function(n) paste(sprintf("%04x", as.integer(runif(n, 0, 65536))), collapse='')
  if (missing(output)) output <- hpath(sprintf("/tmp/io-hmr-temp-%d-%s", Sys.getpid(), .rn(4)))
  if (missing(job.name)) job.name <- sprintf("RCloud:iotools:hmr-%s", .rn(2))
  if (!inherits(input, "HDFSpath")) stop("Sorry, you have to have the input in HDFS for now")
  map.formatter <- NULL
  red.formatter <- NULL
  if (missing(formatter) && inherits(input, "hinput")) map.formatter <- attr(input, "formatter")
  if (!missing(formatter)) {
    if (is.list(formatter)) {
      map.formatter <- formatter$map
      red.formatter <- formatter$reduce
    } else map.formatter <- red.formatter <- formatter
  }
  if (is.null(map.formatter)) map.formatter <- .default.formatter
  if (is.null(red.formatter)) red.formatter <- .default.formatter
  hh <- Sys.getenv("HADOOP_HOME")
  if (!nzchar(hh)) hh <- Sys.getenv("HADOOP_PREFIX")
  if (!nzchar(hh)) hh <- "/usr/lib/hadoop"
  hcmd <- file.path(hh, "bin", "hadoop")
  if (!file.exists(hcmd)) stop("Cannot find working Hadoop home. Set HADOOP_PREFIX if in doubt.")
  sj <- Sys.getenv("HADOOP_STREAMING_JAR")
  if (!nzchar(sj)) sj <- character()
  sj <- Sys.glob(file.path(hh, "contrib", "streaming", "*.jar"))
  if (!length(sj)) {
    ver <- system(paste(shQuote(hcmd), "version"), intern=TRUE)[1L]
    if (!isTRUE(grepl("^Hadoop ", ver)))
      stop("Unable to detect hadoop version via", hcmd, ", if needed set HADOOP_PREFIX accordingly")
    ver <- gsub("^Hadoop ", "", ver)
    if (ver >= "2.0") { ## try to use the class path
      cp <- strsplit(system(paste(shQuote(hcmd), "classpath"), intern=TRUE), .Platform$path.sep, TRUE)[[1]]
      sj <- unlist(lapply(cp, function(o) Sys.glob(file.path(gsub("\\*$","",o), "hadoop-streaming-*.jar"))))
    }
  }
  if (!length(sj)) 
    stop("Cannot find streaming JAR - set HADOOP_STREAMING_JAR or make sure you have a complete Hadoop installation")
  e <- new.env(parent=emptyenv())
  if (!missing(aux)) {
    if (is.list(aux)) for (n in names(aux)) e[[n]] <- aux[[n]]
    else if (is.character(aux)) for (n in aux) e[[n]] <- get(n) else stop("invalid aux")
  }
  e$map.formatter <- map.formatter
  e$red.formatter <- red.formatter
  e$map <- map
  e$reduce <- reduce
  e$load.packages <- packages
  f <- tempfile("hmr-stream-dir")
  dir.create(f,, TRUE, "0700")
  owd <- getwd()
  on.exit(setwd(owd))
  setwd(f)
  save(list=ls(envir=e, all.names=TRUE), envir=e, file="stream.RData")
  map.cmd <- if (identical(map, identity)) "" else if (is.character(map)) paste("-mapper", shQuote(map[1L])) else "-mapper \"R --slave --vanilla -e 'iotools:::run.map()'\""
  reduce.cmd <- if (identical(reduce, identity)) "" else if (is.character(reduce)) paste("-reducer", shQuote(reduce[1L])) else "-reducer \"R --slave --vanilla -e 'iotools:::run.reduce()'\""
  extraD <- if (missing(reducers)) "" else paste0("-D mapred.reduce.tasks=", as.integer(reducers))
  system(paste(
               shQuote(hcmd), "jar", shQuote(sj[1L]),
               "-D", "mapreduce.reduce.input.limit=-1",
               "-D", shQuote(paste0("mapred.job.name=", job.name)), extraD,
               "-input", shQuote(input),
               "-output", shQuote(output),
               "-file", "stream.RData",
               map.cmd, reduce.cmd))
  output
}
