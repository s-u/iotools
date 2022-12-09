chunk.apply <- function(input, FUN, ..., CH.MERGE=rbind, CH.MAX.SIZE=33554432,
                        CH.PARALLEL=1L, CH.SEQUENTIAL=TRUE, CH.BINARY=FALSE,
                        CH.INITIAL=NULL) {
    if (!inherits(input, "ChunkReader"))
        reader <- chunk.reader(input)
    else reader <- input

    if (CH.PARALLEL <= 1L || .Platform$OS.type != "unix") {
        .Call(chunk_apply, reader, CH.MAX.SIZE, CH.MERGE, FUN,
              parent.frame(), .External(pass, ...), CH.BINARY, CH.INITIAL)
    } else {
        worker_queue <- list()
        ## Fill the worker queue.
        for (i in 1:max(CH.PARALLEL, 1)) {
            chunk <- read.chunk(reader, max.size=CH.MAX.SIZE)
            if (length(chunk) == 0) break
            worker_queue[[i]] <- parallel::mcparallel(FUN(chunk, ...), paste0("W", i))
        }
        if (length(worker_queue) == 0) return(CH.MERGE(NULL))
        names(worker_queue) <- paste0("W", seq_along(worker_queue))

        ## Pre-fetch the next chunk if we not at the end of input.
        if (length(chunk) > 0) chunk <- read.chunk(reader, max.size=CH.MAX.SIZE)

        ## Process the chunk-stream.
        ret <- NULL
        done <- FALSE
        while (!done) {
            if (CH.SEQUENTIAL) {
                ## sequential = we have to get the first job so we can merge in sequence
                if (CH.BINARY) {
                    if (!is.null(CH.INITIAL)) {
                        ret <- CH.INITIAL(parallel::mccollect(worker_queue[[1]])[[1]])
                        CH.INITIAL <- NULL
                    } else
                        ret <- CH.MERGE(ret, parallel::mccollect(worker_queue[[1]])[[1]])
                } else {
                    ## sequential, but accumulate
                    ret <- .Call(pl_accumulate, ret, parallel::mccollect(worker_queue[[1]])[[1]])
                }
                worker_queue[1] = NULL
                if (length(chunk) > 0) {
                    worker_queue[[length(worker_queue) + 1L]]  <- parallel::mcparallel(FUN(chunk, ...))
                    chunk <- read.chunk(reader, max.size=CH.MAX.SIZE)
                }
                if (length(worker_queue) == 0)
                    done <- TRUE
            } else {
                ## non-sequential, i.e. we can pick any job that is ready
                res <- parallel::mccollect(worker_queue[lengths(worker_queue) > 0L], FALSE, 1)
                if (length(res)) {
                    ## collect and merge all available results
                    for (worker in names(res)) {
                        if (CH.BINARY) {
                            ret <- CH.MERGE(ret, res[[worker]])
                        } else {
                            ret <- .Call(pl_accumulate, ret, res[[worker]])
                        }
                        if (length(chunk) > 0) {
                            worker_queue[[worker]] <- parallel::mcparallel(FUN(chunk, ...), worker)
                            chunk <- read.chunk(reader, max.size=CH.MAX.SIZE)
                        } else worker_queue[[worker]] <- list()
                    }
                    if (sum(lengths(worker_queue)) == 0)
                        done <- TRUE
                }
            }
        }
        if (CH.BINARY) ret else .Call(pl_call, CH.MERGE, ret, parent.frame())
    }
}

chunk.tapply <- function(input, FUN, ..., sep='\t', CH.MERGE=rbind, CH.MAX.SIZE=33554432) {
    if (!inherits(inherits, "ChunkReader"))
        reader <- chunk.reader(input)
    .Call(chunk_tapply, reader, CH.MAX.SIZE, CH.MERGE, sep, FUN, parent.frame(), .External(pass, ...))
}
