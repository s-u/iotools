#include <Rinternals.h>
#include <unistd.h>

/* stdout() in R only supports text mode, but we need binary */
SEXP stdout_writeBin(SEXP what, SEXP sFlush) {
    if (TYPEOF(what) != RAWSXP) Rf_error("invalid content - must be a raw vector");
    if (LENGTH(what) && write(1, RAW(what), LENGTH(what)) != LENGTH(what))
        Rf_warning("write error while writing to stdout");
    /* NOTE: FD write()s are not buffered, so, effectively,
       sFlush is always treated as TRUE */
    return R_NilValue;
}
