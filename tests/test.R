## small shim to implement testthat expect_* functions
## without the need for all the insane crud of testthat

library(iotools)

expect_equal <- function(expr, result) stopifnot(all.equal(expr, result))
expect_error <- function(expr, error) stopifnot(tryCatch({ x <- expr; FALSE }, error=function(e) length(grep(error, as.character(e))) > 0L))
expect_warning <- function(expr, warn) stopifnot(tryCatch({ x <- expr; FALSE }, warning=function(e) length(grep(warn, as.character(e))) > 0L))

.on.exit <- list()

on_exit <- function(fn) .on.exit <<- c(.on.exit, list(fn))

for (fn in Sys.glob("tests/*.R")) {
    cat("\n\n=== Testing", fn, "===\n")
    source(fn, echo=TRUE)
}

## Long-running tests that we don't want to run on CRAN
if (nchar(Sys.getenv("NOT_CRAN"))) for (fn in Sys.glob("non-cran/*.R")) {
    cat("\n\n=== Testing", fn, "===\n")
    source(fn, echo=TRUE)
}

for (.FUN in .on.exit) .FUN()
