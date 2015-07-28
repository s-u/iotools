#ifndef IOTOOLS_UTILS_H_
#define IOTOOLS_UTILS_H_

#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

double parse_ts(const char *c_start, const char *c_end);
unsigned long count_lines(SEXP sRaw);
unsigned long count_lines_bounded(SEXP sRaw, unsigned long bound);
int Strtoi(const char *nptr, int base);
Rcomplex strtoc(const char *nptr, Rboolean NA);
Rbyte strtoraw (const char *nptr);

static R_INLINE Rboolean Rspace(unsigned int c) {
    return (c == ' ' || c == '\t' || c == '\n' || c == '\r') ? TRUE : FALSE;
}

static R_INLINE long asLong(SEXP x, long NA) {
    double d;
    if (TYPEOF(x) == INTSXP && LENGTH(x) > 0) {
	int res = INTEGER(x)[0];
	return (res == NA_INTEGER) ? NA : res;
    }
    d = asReal(x);
    return (R_finite(d)) ? ((long) d) : NA;
}

#endif
