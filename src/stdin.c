#include <Rinternals.h>
#include <stdio.h>
#include <string.h>

/* 1Mb buffer */
static char buf[1024*1024];

/* FIXME: we may want to deprecate this in favor of file("stdin", "rb")
   unless there is a substantial performance difference. */
SEXP stdin_read(SEXP sN) {
    FILE *f = stdin;
    int n = asInteger(sN), i = 0, incomplete = 0;
    SEXP res = PROTECT(allocVector(STRSXP, n));
    while (i < n && !feof(f) && fgets(buf, sizeof(buf), f)) {
	char *eol = strchr(buf, '\n');
	if (eol) *eol = 0; else incomplete++;
	SET_STRING_ELT(res, i++, mkChar(buf));
    }
    if (i < n) SETLENGTH(res, i);
    UNPROTECT(1);
    if (incomplete) Rf_warning("incomplete lines encountered (%d)", incomplete);
    return res;
}

/* stdout() in R only supports text mode, but we need binary */
SEXP stdout_writeBin(SEXP what, SEXP sFlush) {
    if (TYPEOF(what) != RAWSXP) Rf_error("invalid content - must be a raw vector");
    if (LENGTH(what) && fwrite(RAW(what), LENGTH(what), 1, stdout) != 1)
	Rf_warning("write error while writing to stdout");
    if (asInteger(sFlush)) fflush(stdout);
    return R_NilValue;
}
