chunk.apply <- function(input, FUN, ..., CH.MERGE=rbind, CH.MAX.SIZE=33554432,
                        parallel=1, pipeline=TRUE) {
  if (!inherits(input, "ChunkReader"))
    reader <- chunk.reader(input)
  if (parallel <= 1 || .Platform$OS.type != "unix") {
    return(.Call(chunk_apply, reader, CH.MAX.SIZE, CH.MERGE, FUN, 
                 parent.frame(), .External(pass, ...)))
  } else {
    require(parallel)
    ret=NULL
    done=FALSE
    # If CH.MERGE is list, override it with the correct CH.MERGE function.
    if (identical(list, CH.MERGE)) CH.MERGE = function(x, ...) c(x, list(...))

    if (pipeline) {
      worker_queue = list()

      # Fill the worker queue.
      for (i in 1:max(parallel, 1)) {
        chunk = read.chunk(reader)
        if (length(chunk) == 0) {
          break
        }
        worker_queue[[i]] = mcparallel(FUN(chunk, ...))
      }
      if (length(worker_queue) == 0) return(CH.MERGE(NULL))


      # Pre-fetch the next chunk if we not at the end of input.
      if (length(chunk) > 0) chunk = read.chunk(reader)

      # Process the chunk-stream.
      while (!done) {
        ret = CH.MERGE(ret, mccollect(worker_queue[[1]])[[1]])
        worker_queue[1] = NULL
        if (length(chunk) > 0) {
          worker_queue[[length(worker_queue)+1]] = mcparallel(FUN(chunk, ...))
          chunk = read.chunk(reader)
        }
        if (length(worker_queue) == 0) done = TRUE
      }
    } else {
      chunks = list()
      while(!done) {
        for (i in 1:parallel) chunks[[i]] = read.chunk(reader)
        if (any(unlist(lapply(chunks, length)) == 0)) done=TRUE
        ret = CH.MERGE(ret, mclapply(chunks, FUN, mc.cores=parallel))
      }
    }
    return(ret)
  }
}

chunk.tapply <- function(input, FUN, ..., sep='\t', CH.MERGE=rbind, CH.MAX.SIZE=33554432) {
  if (!inherits(inherits, "ChunkReader"))
    reader <- chunk.reader(input)
  .Call(chunk_tapply, reader, CH.MAX.SIZE, CH.MERGE, sep, FUN, parent.frame(), .External(pass, ...))
}
