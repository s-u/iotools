library(iotools)

counts <- c(0L, 0L)

group <- function(grp) {
  if (any(counts > 0)) {
    cat("\n Summary:", counts[2], "OK,", counts[1],"FAILED\n")
    if (counts[1] > 0) stop("*** At least one test failed")
  }
  counts <<- c(0L, 0L)
  cat("\n===", grp, "===\n\n")
}

.timing <- numeric()

test <- function(txt, val, res) {
    cat(txt, "...")
    ok <- if (missing(res)) isTRUE(val) else isTRUE(all.equal(val, res))
    tim <- if (length(.timing)) paste0("   (",paste(as.integer(.timing * 1000),collapse='/'),")") else ""
    ## \x1b[1m and \x1b[m for ANSI if we care...
    cat(if (ok) " OK" else " FAILED", tim, "\n", sep='')
    if (!ok) {
        if (missing(res)) {
            cat("*** expected TRUE, got:")
            str(val)
            cat("*** call: ", deparse(substitute(val)), "\n\n")
        } else {
            cat("*** expected: ")
            str(val)
            cat("***      got: ")
            str(res)
            cat("***all.equal: ")
            print(all.equal(val, res))
            cat("***     call: ", deparse(substitute(res)), "\n\n")
        }
    }
    #if (length(.timing)) print(.timing)
    .timing <<- numeric()
    counts[ok + 1] <<- counts[ok + 1] + 1L
}

set.seed(123)

cat(" - Generating large file ... ")
# fn <- tempfile("test", fileext=".csv")
if (file.exists("test.csv")) unlink("test.csv")
fn <- "test.csv"

.max.ch <- 4e6 ## want more than one key

res <- sapply(LETTERS, function(i) {
    n = abs(rnorm(1,100) + 60000) + 1L
    d=data.frame(A=i, B=rnorm(n), C=runif(n))
    write.table(d, fn, TRUE, FALSE, "\t", row.names=FALSE, col.names=FALSE)
    c(B=sum(d$B), C=sum(d$C))
})

cat(sprintf("%.1fMb\n", file.info(fn)$size / (1024^2)))

test("chunk.apply", as.integer(file.info(fn)$size), {
    f <- file(fn, "rb")
    cr <- chunk.reader(f)
    .timing <<- system.time(
        s <- chunk.apply(cr, length, CH.MERGE=sum, CH.MAX.SIZE=.max.ch)
    )
    rm(cr)
    close(f)
    gc()
    s
})

test("parallel chunk.apply", as.integer(file.info(fn)$size), {
    f <- file(fn, "rb")
    cr <- chunk.reader(f)
    .timing <<- system.time(
        s <- chunk.apply(cr, length, CH.MERGE=sum, CH.MAX.SIZE=.max.ch, CH.PARALLEL=4)
    )
    rm(cr)
    close(f)
    gc()
    s
})

test("chunk.apply with keys + ctapply", res[1,], {
    f <- file(fn, "rb")
    cr <- chunk.reader(f, sep="\t")
    .timing <<- system.time(
        s <- chunk.apply(cr, function(r) {
            d <- dstrsplit(r, list(A="",B=1,C=1), "\t")
            ctapply(d$B, d$A, sum)
        }, CH.MERGE=c, CH.MAX.SIZE=.max.ch)
    )
    rm(cr)
    close(f)
    gc()
    s
})

test("chunk.apply with keys + matrix ctapply", res, {
    f <- file(fn, "rb")
    cr <- chunk.reader(f, sep="\t")
    .timing <<- system.time(
        s <- chunk.apply(cr, function(r) {
            d <- dstrsplit(r, list(A="",B=1,C=1), "\t")
            m <- ctapply(as.matrix(d[,c("B","C")]), d$A, colSums, MERGE=rbind)
        }, CH.MERGE=rbind, CH.MAX.SIZE=.max.ch)
    )
    rm(cr)
    close(f)
    gc()
    colnames(s) <- c("B","C")
    t(s)
})

test("chunk.tapply with matrix", res, {
    f <- file(fn, "rb")
    cr <- chunk.reader(f, sep="\t")
    .timing <<- system.time(
        s <- chunk.tapply(cr, function(r) {
            d <- dstrsplit(r, list(A="",B=1,C=1), "\t")
            m <- ctapply(as.matrix(d[,c("B","C")]), d$A, colSums, MERGE=rbind)
        }, CH.MERGE=rbind, CH.MAX.SIZE=.max.ch)
    )
    rm(cr)
    close(f)
    gc()
    colnames(s) <- c("B","C")
    t(s)
})



test("chunk.apply with keys + slow matrix ctapply, parallel", res, {
    f <- file(fn, "rb")
    cr <- chunk.reader(f, sep="\t")
    .timing <<- system.time(
        s <- chunk.apply(cr, function(r) {
            d <- dstrsplit(r, list(A="",B=1,C=1), "\t")
            m <- ctapply(as.matrix(d[,c("B","C")]), d$A, function(m) apply(m, 2, sum), MERGE=rbind)
        }, CH.MERGE=rbind, CH.MAX.SIZE=.max.ch, CH.PARALLEL=4)
    )
    rm(cr)
    close(f)
    gc()
    colnames(s) <- c("B","C")
    t(s)
})

unlink(fn)
