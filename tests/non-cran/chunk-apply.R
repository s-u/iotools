
cat(" - Generating large file ...")
fn <- tempfile("test", fileext=".csv")

on_exit(function() unlink(fn))

for (i in LETTERS) {
    n = abs(rnorm(1) + 10000) + 1L
    d=data.frame(A=i, B=rnorm(n), C=runif(n))
    write.table(d, fn, TRUE, FALSE, "\t", row.names=FALSE, col.names=FALSE)
}

f <- file(fn, "rb")
cr <- chunk.reader(f)
s <- chunk.apply(cr, length, CH.MERGE=sum, CH.MAX.SIZE=1e6)
rm(cr)
close(f)
gc()

expect_equal(s, file.info(fn)$size)

f <- file(fn, "rb")
cr <- chunk.reader(f)
s <- chunk.apply(cr, length, CH.MERGE=sum, CH.MAX.SIZE=1e6, CH.PARALLEL=4)
rm(cr)
close(f)
gc()

expect_equal(s, file.info(fn)$size)

