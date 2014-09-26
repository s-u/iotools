hpath <- function(path) structure(path, class="HDFSpath")

.default.formatter <- function(x) {
  y <- mstrsplit(x, "|", "\t")
  if (ncol(y) == 1L) y[, 1] else y
}

hinput <- function(path, formatter=.default.formatter)
  structure(path, class=c("hinput", "HDFSpath"), formatter=formatter)

.hadoop.detect <- function() {
  hh <- Sys.getenv("HADOOP_HOME")
  if (!nzchar(hh)) hh <- Sys.getenv("HADOOP_PREFIX")
  if (!nzchar(hh)) hh <- "/usr/lib/hadoop"
  hcmd <- file.path(hh, "bin", "hadoop")
  if (!file.exists(hcmd)) stop("Cannot find working Hadoop home. Set HADOOP_PREFIX if in doubt.")

  sj <- Sys.getenv("HADOOP_STREAMING_JAR")
  if (!nzchar(sj)) sj <- Sys.glob(file.path(hh, "contrib", "streaming", "*.jar"))
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
  list(hh=hh, hcmd=hcmd, sj=sj)
}

c.hinput = function(..., recursive = FALSE) {
  if(!all(sapply(list(...), function(v) all.equal(attr(v, "class"), c("hinput", "HDFSpath"))) == TRUE)) {
    warning("Using default combine function as not all objects are of class hinput")
    return(c(sapply(list(...), function(v) as.character(v))))
  }
  formatters = sapply(list(...), function(v) attr(v, "formatter"))
  if(!all(sapply(formatters, all.equal, formatters[[1]]))) {
    stop("All input objects must have the same formatters")
  }

  return(structure(unique(c(sapply(list(...), function(v) as.character(v)))),
         class=c("hinput", "HDFSpath"), formatter=formatters[[1]]))
}

hmr <- function(input, output, map=identity, reduce=identity, job.name, aux, formatter, packages=loadedNamespaces(), reducers,
                remote, wait=TRUE, hadoop.conf, hadoop.opt, R="R", verbose=TRUE) {
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

  if (missing(remote)) {
    h <- .hadoop.detect()
    hh <- h$hh
    hcmd <- h$hcmd
    sj <- h$sj
    if (!length(sj))
      stop("Cannot find streaming JAR - set HADOOP_STREAMING_JAR or make sure you have a complete Hadoop installation")
  }

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
  map.cmd <- if (identical(map, identity)) "-mapper cat" else if (is.character(map)) paste("-mapper", shQuote(map[1L])) else paste0("-mapper \"",R," --slave --vanilla -e 'iotools:::run.map()'\"")
  reduce.cmd <- if (identical(reduce, identity)) "" else if (is.character(reduce)) paste("-reducer", shQuote(reduce[1L])) else paste0("-reducer \"",R," --slave --vanilla -e 'iotools:::run.reduce()'\"")
  extraD <- if (missing(reducers)) "" else paste0("-D mapred.reduce.tasks=", as.integer(reducers))
  if (!missing(hadoop.opt) && length(hadoop.opt)) {
    hon <- names(hadoop.opt)
    extraD <- if (is.null(hon))
      paste(extraD, paste(as.character(hadoop.opt), collapse=" "))
    else
      paste(extraD, paste("-D", shQuote(paste0(hon, "=", as.character(hadoop.opt))), collapse=" "))
  }

  hargs <- paste(
               "-D", "mapreduce.reduce.input.limit=-1",
               "-D", shQuote(paste0("mapred.job.name=", job.name)), extraD,
               paste("-input", shQuote(input), collapse=' '),
               "-output", shQuote(output),
               "-file", "stream.RData",
               map.cmd, reduce.cmd)
  cfg <- ""
  if (!missing(hadoop.conf)) cfg <- paste("--config", shQuote(hadoop.conf)[1L])
  if (missing(remote)) {
    h0 <- paste(shQuote(hcmd), cfg, "jar", shQuote(sj[1L]))
    cmd <- paste(h0, hargs)
    system(cmd, wait=wait, ignore.stdout = !verbose, ignore.stderr = !verbose)
  } else {
    if (is.character(remote)) {
      auth.user <- NULL
      auth.pwd <- NULL
      if (file.exists("~/.hmr.json")) {
        cfg <- rjson::fromJSON(paste(readLines("~/.hmr.json"), collapse='\n'))
        if (length(cfg)) {
          hc <-cfg[[remote, exact=TRUE]]
          if (length(hc)) {
            host <- hc$host
            port <- hc$port
            user <- hc$user
            tls <- hc$tls
            pwd <- hc$password
            if (is.null(port)) port <- 6311L
            if (is.null(tls)) tls <- FALSE
            remote <- RSclient::RS.connect(host, port, tls)
            on.exit(RSclient::RS.close(remote))
            if (!is.null(user))
              RSclient::RS.login(remote, user, pwd, authkey=RSclient::RS.authkey())
          } else if (length(hc <- cfg[["*", exact=TRUE]])) {
            auth.user <- hc$user
            auth.pwd <- hc$password
          }
        }
      } else {
        remote <- RSclient::RS.connect(remote)
        on.exit(RSclient::RS.close(remote))
        if (!is.null(auth.user))
          RSclient::RS.login(remote, auth.user, auth.pwd, authkey=RSclient::RS.authkey())
      }
    }
    if (!inherits(remote, "RserveConnection"))
      stop("remote must be an RserveConnection or a string denoting a server to connect to")
    l <- list(stream=readBin("stream.RData", raw(), file.info("stream.RData")$size),
              hargs=hargs, hcfg=cfg)
    RSclient::RS.eval(remote, as.call(list(quote(iotools:::.remote.cmd), l)), wait=wait, lazy=FALSE)
  }
  output
}

.remote.cmd <- function(args) {
  f <- tempfile("hmr-stream-dir")
  dir.create(f,, TRUE, "0700")
  owd <- getwd()
  on.exit(setwd(owd))
  setwd(f)
  writeBin(args$stream, "stream.RData")
  hargs <- args$hargs

  h <- .hadoop.detect()
  if (!length(h$sj))
    stop("Cannot find streaming JAR - set HADOOP_STREAMING_JAR or make sure you have a complete Hadoop installation")
  h0 <- paste(shQuote(h$hcmd), args$hcfg, "jar", shQuote(h$sj[1L]))
  cmd <- paste(h0, hargs)
  ## FIXME: we could pass-through wait so that we get notified if the submission fails
  ##        but it may be slow if stream.RData is huge
  system(cmd)
}
