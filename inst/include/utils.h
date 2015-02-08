#ifndef IOTOOLS_UTILS_H_
#define IOTOOLS_UTILS_H_

#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

double parse_ts(const char *c_start, const char *c_end);
long count_lines(SEXP sRaw);
long count_lines_bounded(SEXP sRaw, int bound);
int Strtoi(const char *nptr, int base);
Rcomplex strtoc(const char *nptr, Rboolean NA);
R_INLINE Rboolean Rspace(unsigned int c);
Rbyte strtoraw (const char *nptr);

#endif