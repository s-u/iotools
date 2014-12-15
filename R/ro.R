## this gets called for a chunk in the Rserve instance
.ro.chunk <- function(workdir, FUN, chunk, formatter) {
  on.exit(q("no"))
  setwd(workdir)
  load("stream.RData", .GlobalEnv)
  if (!is.null(.GlobalEnv$load.packages)) try(for(i in .GlobalEnv$load.packages) require(i, quietly=TRUE, character.only=TRUE), silent=TRUE)
  env <- new.env(parent=.GlobalEnv)
  message("running FUN()\n")
  env$.self <- env
  environment(FUN) <- env
  print(eval(bquote(.(FUN)(.(formatter)(.(chunk)))), env))

  key <- .GlobalEnv$.ro.info$acckey
  message("listening on ", .GlobalEnv$.ro.info$host, "port", .GlobalEnv$.ro.info$port, "\n")
  s <- socketConnection(.GlobalEnv$.ro.info$host, .GlobalEnv$.ro.info$port, TRUE, TRUE, "a+b")
  message("reading...\n")
  while (length(len <- readBin(s, 1L, 1L)) &&
         length(buf <- readBin(s, raw(), len)) == len) {
    l <- unserialize(buf)
    if (!is.list(l))
      stop("invalid payload")
    if (!identical(l$key, key))
      stop("invalid key")
    res <- if (is.language(l$exp))
      eval(l$exp, env)
    else if (is.function(l$fun))
      l$fun()
    else {
      message("no command, shutting down")
      close(s)
      return(NULL)
    }
    res <- serialize(res, NULL)
    writeBin(length(res), s)
    writeBin(res, s)
  }
}

## create a new worker
.ro.new <- function() {
  sock <- file.path(unixtools:::user.info()[,"home"], "rserve.sock")
  #sock <- path.expand("~/rserve.sock") # paste0("/tmp/",Sys.getenv("USER"),"-rserve.sock") 
  c <- tryCatch(RSclient::RS.connect(sock, 0L), error=function(e) {
    Rserve::Rserve(args=c("--RS-socket", sock, "--no-save"))
    if (!file.exists(sock)) Sys.sleep(0.2)
    RSclient::RS.connect(sock, 0L)
  })
  port <- as.integer(runif(1) * 32768) + 16384L
  host <- system("hostname -f", TRUE)
  acckey <- paste(letters[as.integer(runif(40) * 25) + 1L], collapse='')
  RSclient::RS.eval.qap(c, bquote({.GlobalEnv$.ro.info <- list(host=.(host), port=.(port), acckey=.(acckey)); TRUE }))
  attr(c, "url") <- paste0("roctopus://", host, ":", port, "/", acckey)
  c
}

worker <- function(where) {
  if (!all(grepl("^roctopus://",where))) stop("expecting ROctopus URL")
  a <- strsplit(gsub("^roctopus://","",where), "[:/]")[[1]]
              
  s <- socketConnection(a[1], as.integer(a[2]), FALSE, TRUE, "a+b")
  structure(list(s=s, key=a[3]), class="workerHandle")
}

.msg <- function(s, l, wait=TRUE) {
  r <- serialize(l, NULL)
  writeBin(length(r), s)
  writeBin(r, s)
  if (wait) {
    if (!length(len <- readBin(s, 1L, 1L)))
      stop("nothing to read from the worker")
    r <- readBin(s, raw(), len)
    if (length(r) != len)
      stop("incomplete response form the worker")
    unserialize(r)
  }
}

weval <- function(worker, exp, wait=TRUE)
    .msg(worker$s, list(exp=substitute(exp), key=worker$key), wait=wait)

wcall <- function(worker, call, wait=TRUE)
    .msg(worker$s, list(exp=call, key=worker$key), wait=wait)

wclose <- function(worker) {
    writeBin(0L, worker$s)
      close(worker$s)
  }

wready <- function(workers, timeout = NULL) {
    if (inherits(workers, "workerHandle")) workers <- list(workers) ## wrap singletons
      sl <- lapply(workers, function(o) o$s)
      which(socketSelect(sl, FALSE, timeout))
  }

wresult <- function(worker) {
    len <- readBin(worker$s, 1L, 1L)
      if (!length(len)) stop("nothing to get from the worker")
      if (length(r <- readBin(worker$s, raw(), len)) != len)
            stop("incomplete answer from the worker")
      unserialize(r)
  }

wqapply <- function(workers, ..., fold) {
    for (.worker in workers) weval(.worker, ..., wait=FALSE)
      if (missing(fold)) lapply(workers, wresult) else do.call(fold, lapply(workers, wresult))
  }

wapply <- function(workers, ..., fold) {
    for (.worker in workers) wcall(.worker, ..., wait=FALSE)
      if (missing(fold)) lapply(workers, wresult) else do.call(fold, lapply(workers, wresult))
  }

wrun <- function(workers, exp) {
    for (.worker in workers) wcall(.worker, bquote({ .(exp); TRUE }), wait=FALSE)
      all(unlist(lapply(workers, wresult)))
  }
