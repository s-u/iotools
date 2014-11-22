ioreadBlock <- function(input, block=100000L) {
}

iostream <- function(expr, sep="|", input=file("stdin"), output=file("stdout"), block=100000L) {
}

as.output <- function(x) UseMethod("as.output")
as.output.default <- function(x) if (is.null(names(x))) as.character(x) else paste(names(x), as.character(x), sep='\t')
as.output.table <- function(x) paste(names(x), x, sep='\t')
as.output.matrix <- function(x) { o <- apply(x, 1, paste, collapse='|'); if (!is.null(rownames(x))) o <- paste(rownames(x), o, sep='\t'); o }
as.output.list <- function(x) paste(names(x), sapply(x, function (e) paste(as.character(e), collapse='|')), sep='\t')
as.output.data.frame <- function(x) { if (ncol(x) == 1L) return(as.character(x[,1])); o <- apply(x[,-1,drop=FALSE], 1, paste, collapse='|'); o <- paste(x[,1], o, sep='\t'); o }

## this is almost like run.chunked() except for passing it to the worker
run.persistent <- function(FUN, formatter=mstrsplit, key.sep=NULL) {
  wd <- getwd()
  load("stream.RData", .GlobalEnv)
  ## if (!is.null(.GlobalEnv$load.packages)) try(for(i in .GlobalEnv$load.packages) require(i, quietly=TRUE, character.only=TRUE), silent=TRUE)
  input <- file("stdin", "rb")
  output <- stdout()
  reader <- chunk.reader(input, sep=key.sep)
  ws <- character()
  while (TRUE) {
     chunk <- read.chunk(reader)
     if (!length(chunk)) break
     c <- .ro.new()
     ws <- c(ws, attr(c, "url"))
     tryCatch(RSclient::RS.eval(c, bquote(iotools:::.ro.chunk(.(wd), .(FUN), .(chunk), .(formatter))), wait=FALSE, lazy=FALSE),
              error=function(...) NULL)
     tryCatch(RSclient::RS.close(c), error=function(...) NULL)
   }
  writeLines(ws)
  invisible(TRUE)
}

run.chunked <- function(FUN, formatter=mstrsplit, key.sep=NULL) {
  ## we have to load stream.RData first since it actually contains FUN (thank you lazy evaluation - the only reason why this works at all)
  load("stream.RData", .GlobalEnv)

  ## FIXME: it would be nice to test for 'identity' but since this may have been serialized, identical() may not help here ...
  if (is.null(FUN) || identical(FUN, identity)) { ## pass-through, no chunking, just behave like `cat`
    input <- file("stdin", "rb")
    N <- 16777216L ## 16Mb
    while (length(buf <- readBin(input, raw(), N))) .Call(stdout_writeBin, buf, FALSE)
    .Call(stdout_writeBin, raw(), TRUE) ## just a flush
    return(invisible(TRUE))
  }

  if (!is.null(.GlobalEnv$load.packages)) try(for(i in .GlobalEnv$load.packages) require(i, quietly=TRUE, character.only=TRUE), silent=TRUE)
  input <- file("stdin", "rb")
  output <- stdout()
  reader <- chunk.reader(input, sep=key.sep)
  parallel <- if (!is.null(.GlobalEnv$parallel.chunks)) as.integer(.GlobalEnv$parallel.chunks)[1L] else 1L
  if (is.na(parallel) || parallel < 2L) {
    while (TRUE) {
      chunk <- read.chunk(reader)
      if (!length(chunk)) break
      writeLines(as.output(FUN(formatter(chunk))), output)
    }
  } else {
    require(parallel)
    pj <- replicate(parallel, integer())
    i <- 1L
    while (TRUE) {
      chunk <- read.chunk(reader)
      if (!length(chunk)) break
      if (inherits(pj[[i]], "parallelJob")) {
        writeLines(mccollect(pj[[i]]), output)
	mccollect(pj[[i]]) ## close child
	pj[[i]] <- integer()
      }
      pj[[i]] <- mcparallel(as.output(FUN(formatter(chunk))))
      i <- (i %% parallel) + 1L
    }
    ## collect all outstanding jobs
    while (inherits(pj[[i]], "parallelJob")) {
      writeLines(mccollect(pj[[i]]), output)
      mccollect(pj[[i]]) ## close child
      pj[[i]] <- integer()
      i <- (i %% parallel) + 1L
    }
  }
  invisible(TRUE)
}

run.map <- function() run.chunked(.GlobalEnv$map, .GlobalEnv$map.formatter)
run.reduce <- function() run.chunked(.GlobalEnv$reduce, .GlobalEnv$red.formatter, "\t")
run.ro <- function() run.persistent(.GlobalEnv$map, .GlobalEnv$map.formatter)

chunk.apply <- function(input, FUN, ..., CH.MERGE=rbind, CH.MAX.SIZE=33554432,
                        parallel=1) {
  if (parallel < 1)
    stop("You must specify a postive integer number of parallel processes")
  if (parallel == 1) 
    return(seq.chunk.apply(input, FUN, ..., CH.MERGE=CH.MERGE, 
                           CH.MAX.SIZE=CH.MAX.SIZE, frame=parent.frame()))
  # Multiple processes have been specified.
  if (.Platform$OS.type != "unix") {
    warning(paste("Parallel chunk.apply only work on unix platforms.",
                  "Running sequentially"))
    return(seq.chunk.apply(input, FUN, ..., CH.MERGE=CH.MERGE, 
                           CH.MAX.SIZE=CH.MAX.SIZE, frame=parent.frame()))
  }
  mc.chunk.apply(input, FUN, ..., CH.MERGE=CH.MERGE, 
                 CH.MAX.SIZE=CH.MAX.SIZE, mc.cores=parallel)
}

seq.chunk.apply <- function(input, FUN, ..., CH.MERGE, CH.MAX.SIZE, frame) {
  if (!inherits(input, "ChunkReader"))
    reader <- chunk.reader(input)
  .Call(chunk_apply, reader, CH.MAX.SIZE, CH.MERGE, FUN, frame, 
        .External(pass, ...))
}

mc.chunk.apply = function(input, FUN, ..., CH.MERGE, CH.MAX.SIZE, 
                          mc.cores) {
  require(parallel)
  if (!inherits(input, "ChunkReader"))
    reader = chunk.reader(input)
  if (mc.cores == 1) {
    warning("Only 1 core specified, calling chunk.apply")
    return(chunk.apply(input, FUN, ..., CH.MERGE, CH.MAX.SIZE))
  }
  worker_queue = list()

  # Fill the worker queue.
  for (i in 1:max(mc.cores-1, 1)) {
    chunk = read.chunk(reader)
    if (length(chunk) == 0) {
      break
    }
    worker_queue[[i]] = mcparallel(FUN(chunk, ...))
  }
  if (length(worker_queue) == 0) return(CH.MERGE(NULL))
 
  # If CH.MERGE is list, override it with the correct CH.MERGE function.
  if (identical(list, CH.MERGE))
    CH.MERGE = function(x, ...) c(x, list(...))
    
  # Pre-fetch the next chunk if we not at the end of input.
  if (length(chunk) > 0) chunk = read.chunk(reader)

  # Process the chunk-stream.
  done = FALSE
  ret = NULL
  while (!done) {
    ret = CH.MERGE(ret, mccollect(worker_queue[[1]])[[1]])
    worker_queue[1] = NULL
    if (length(chunk) > 0) {
      worker_queue[[length(worker_queue)+1]] = mcparallel(FUN(chunk, ...))
      chunk = read.chunk(reader)
    }
    if (length(worker_queue) == 0) done = TRUE
  }
  ret
}

chunk.tapply <- function(input, FUN, ..., sep='\t', CH.MERGE=rbind, CH.MAX.SIZE=33554432) {
  if (!inherits(inherits, "ChunkReader"))
    reader <- chunk.reader(input)
  .Call(chunk_tapply, reader, CH.MAX.SIZE, CH.MERGE, sep, FUN, parent.frame(), .External(pass, ...))
}
