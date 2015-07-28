#define USE_RINTERNALS 1
#include "utils.h"

/* from tparse.c (which is based on code from fasttime) */
double parse_ts(const char *c_start, const char *c_end);

unsigned long count_lines(SEXP sRaw) {
    const char *c = (const char*) RAW(sRaw);
    const char *e = c + XLENGTH(sRaw);
    unsigned long lines = 0;
    while ((c = memchr(c, '\n', e - c))) {
      lines++;
      c++;
    }
    if (e > c && e[-1] != '\n') lines++;
    return lines;
}

unsigned long count_lines_bounded(SEXP sRaw, unsigned long bound) {
    const char *c = (const char*) RAW(sRaw);
    const char *e = c + XLENGTH(sRaw);
    unsigned long lines = 0;
    while ((c = memchr(c, '\n', e - c)) && lines < bound) {
	lines++;
	c++;
    }
    if (e > c && e[-1] != '\n') lines++;
    return lines;
}

/* Like strtol, but for ints not longs and returns NA_INTEGER on overflow */
int Strtoi(const char *nptr, int base)
{
    long res;
    char *endp;

    errno = 0;
    res = strtol(nptr, &endp, base);
    if (*endp != '\0') res = NA_INTEGER;
    /* next can happen on a 64-bit platform */
    if (res > INT_MAX || res < INT_MIN) res = NA_INTEGER;
    if (errno == ERANGE) res = NA_INTEGER;
    return (int) res;
}

Rcomplex strtoc(const char *nptr, Rboolean NA)
{
    Rcomplex z;
    double x, y;
    char *s, *endp;

    x = R_strtod(nptr, &endp);
    if (isBlankString(endp)) {
      z.r = x; z.i = 0;
    } else if (*endp == 'i')  {
      z.r = 0; z.i = x;
      endp++;
    } else {
      s = endp;
      y = R_strtod(s, &endp);
      if (*endp == 'i') {
          z.r = x; z.i = y;
          endp++;
      } else {
          z.r = 0; z.i = 0;
          endp = (char *) nptr; /* -Wall */
      }
    }
    return z;
}

Rbyte strtoraw (const char *nptr)
{
    const char *p = nptr;
    int i, val = 0;

    /* should have whitespace plus exactly 2 hex digits */
    while(Rspace(*p)) p++;
    for(i = 1; i <= 2; i++, p++) {
      val *= 16;
      if(*p >= '0' && *p <= '9') val += *p - '0';
      else if (*p >= 'A' && *p <= 'F') val += *p - 'A' + 10;
      else if (*p >= 'a' && *p <= 'f') val += *p - 'a' + 10;
      else {val = 0; break;}
    }
    return (Rbyte) val;
}

