#include <Rinternals.h>
#include <stdio.h>
#include <string.h>

/* 1Mb buffer */
static char buf[1024*1024];

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
