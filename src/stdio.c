#include <Rinternals.h>
#include <unistd.h>

/* stdout() in R only supports text mode, but we need binary */
SEXP stdout_writeBin(SEXP what, SEXP sFlush) {
    if (TYPEOF(what) != RAWSXP) Rf_error("invalid content - must be a raw vector");
    if (LENGTH(what) && write(1, RAW(what), LENGTH(what)) != LENGTH(what))
        Rf_warning("write error while writing to stdout");
    if (asInteger(sFlush)) fflush(stdout);
    return R_NilValue;
}
