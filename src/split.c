#include <Rinternals.h>
#include <string.h>
#include <stdlib.h>

#define FL_RESILIENT 1 /* do not fail, proceed even if the input has more columns */

#include "utils.h"

SEXP mat_split(SEXP s, SEXP sSep, SEXP sNamesSep, SEXP sResilient, SEXP sNcol,
               SEXP sWhat, SEXP sSkip, SEXP sNlines, SEXP sQuote) {
  unsigned int ncol = 1, np = 0, resilient = asInteger(sResilient);
  unsigned long nrow, i, k, N, len, quoteLen;
  int use_ncol = asInteger(sNcol);
  int nsep = -1;
  long skip = asLong(sSkip, 0);
  long nlines = asLong(sNlines, -1);
  SEXP res, rnam;
  char sep;
  char num_buf[48];
  /* sraw/send is only used for raw vector parsing, but we have to set it to 0
     to make gcc happy which cannot figure out that it's actually unused */
  const char *c, *c2, *sraw = 0, *send = 0, *l, *le, *quoteChars;

  /* parse sep input */
  if (TYPEOF(sNamesSep) == STRSXP && LENGTH(sNamesSep) > 0)
    nsep = (int) (unsigned char) *CHAR(STRING_ELT(sNamesSep, 0));
  if (TYPEOF(sSep) != STRSXP || LENGTH(sSep) < 1)
    Rf_error("invalid separator");
  sep = CHAR(STRING_ELT(sSep, 0))[0];

  /* parse quote information */
  quoteChars = CHAR(STRING_ELT(sQuote, 0));
  quoteLen = strlen(quoteChars);

  /* check the input data */
  if (TYPEOF(s) == RAWSXP) {
    nrow = (nlines >= 0) ? count_lines_bounded(s, nlines + skip) : count_lines(s);
    sraw = (const char*) RAW(s);
    send = sraw + XLENGTH(s);
    if (nrow >= skip) {
	unsigned long slen = XLENGTH(s);
	nrow = nrow - skip;
	i = 0;
	while (i < skip && (sraw = memchr(sraw, '\n', slen))) { sraw++; i++; }
    } else {
	nrow = 0;
	sraw = send;
    }
  } else if (TYPEOF(s) == STRSXP) {
    nrow = LENGTH(s);
    if (nrow >= skip) {
      nrow -= skip;
    } else {
      skip = nrow;
      nrow = 0;
    }
  } else {
    Rf_error("invalid input to split - must be a raw or character vector");
  }
  if (nlines >= 0 && nrow > nlines) nrow = nlines;

  /* If no rows left, return an empty matrix */
  if (!nrow) {
    if (np) UNPROTECT(np);
    return allocMatrix(TYPEOF(sWhat), 0, 0);
  }

  /* count number of columns */
  if (use_ncol < 1) {
    if (TYPEOF(s) == RAWSXP) {
      c = sraw;
      le = memchr(sraw, '\n', send - sraw);
      if (TYPEOF(sWhat) == STRSXP && quoteLen) {
        ncol = 0;
        while(1)
        {
          ncol++;
          for (k = 0; k < quoteLen; k++) {
            if (*c == quoteChars[k]) {
              c++;
              if (! (c = memchr(c, quoteChars[k], le - c)))
                Rf_error("End of line within quote string on line 1; cannot determine num columns!");
              break; /* note: breaks inner 'for' loop, not 'if' statement */
            }
          }
          if (!(c = (memchr(c, (unsigned char) sep, le - c))))
            break;
          c++;
        }
      } else {
        ncol = 1;
        while ((c = memchr(c, (unsigned char) sep, le - c))) { ncol++; c++; }
      }
    } else {
      c = CHAR(STRING_ELT(s, 0));
      if (TYPEOF(sWhat) == STRSXP && quoteLen) {
        ncol = 0;
        while(1)
        {
          ncol++;
          for (k = 0; k < quoteLen; k++) {
            if (*c == quoteChars[k]) {
              c++;
              if (!(c = strchr(c, quoteChars[k])))
                Rf_error("End of line within quote string on line 1; cannot determine num columns!");
              break; /* note: breaks inner 'for' loop, not 'if' statement */
            }
          }
          if (!(c = (strchr(c, sep))))
            break;
          c++;
        }
      } else {
        while ((c = strchr(c, sep))) { ncol++; c++; }
      }
    }
    /* if sep and nsep are the same then the first "column" is the key and not the column */
    if (nsep == (int) (unsigned char) sep) ncol--;
  } else ncol = use_ncol;

  /* allocate space for the result */
  N = ncol * nrow;
  switch(TYPEOF(sWhat)) {
    case LGLSXP:
    case INTSXP:
    case REALSXP:
    case CPLXSXP:
    case STRSXP:
    case RAWSXP:
      res = PROTECT(allocMatrix(TYPEOF(sWhat), nrow, ncol));
      break;

    default:
      Rf_error("Unsupported input to what.");
      break;
  }
  if (nsep >= 0) {
    SEXP dn;
    setAttrib(res, R_DimNamesSymbol, (dn = allocVector(VECSXP, 2)));
    SET_VECTOR_ELT(dn, 0, (rnam = allocVector(STRSXP, nrow)));
  }
  np++;

  /* cycle over the rows and parse the data */
  for (i = 0; i < nrow; i++) {
    int j = i;

    /* find the row of data */
    if (TYPEOF(s) == RAWSXP) {
        l = sraw;
        le = memchr(l, '\n', send - l);
        if (!le) le = send;
        sraw = le + 1;
        if (*(le - 1) == '\r' ) le--; /* account for DOS-style '\r\n' */
    } else {
        l = CHAR(STRING_ELT(s, i + skip));
        le = l + strlen(l);
    }

    /* if nsep, load rowname */
    if (nsep >= 0) {
      c = memchr(l, nsep, le - l);
      if (c) {
        SET_STRING_ELT(rnam, i, Rf_mkCharLen(l, c - l));
        l = c + 1;
      } else
        SET_STRING_ELT(rnam, i, R_BlankString);
    }

    /* now split the row into elements */
    while (l < le) {
      if (!(c = memchr(l, sep, le - l)))
        c = le;

      if (j >= N) {
        if (resilient) break;
        Rf_error("line %lu: too many columns (expected %u)", (unsigned long)(i + 1), ncol);
      }

      switch(TYPEOF(sWhat)) {
      case LGLSXP:
        len = (int) (c - l);
        if (len > sizeof(num_buf) - 1)
            len = sizeof(num_buf) - 1;
        memcpy(num_buf, l, len);
        num_buf[len] = 0;
        int tr = StringTrue(num_buf), fa = StringFalse(num_buf);
        LOGICAL(res)[j] = (tr || fa) ? tr : NA_INTEGER;
        break;

      case INTSXP:
        len = (int) (c - l);
        /* watch for overflow and truncate -- should we warn? */
        if (len > sizeof(num_buf) - 1)
            len = sizeof(num_buf) - 1;
        memcpy(num_buf, l, len);
        num_buf[len] = 0;
        INTEGER(res)[j] = Strtoi(num_buf, 10);
        break;

      case REALSXP:
        len = (int) (c - l);
        /* watch for overflow and truncate -- should we warn? */
        if (len > sizeof(num_buf) - 1)
            len = sizeof(num_buf) - 1;
        memcpy(num_buf, l, len);
        num_buf[len] = 0;
        REAL(res)[j] = R_atof(num_buf);
        break;

      case CPLXSXP:
        len = (int) (c - l);
        /* watch for overflow and truncate -- should we warn? */
        if (len > sizeof(num_buf) - 1)
            len = sizeof(num_buf) - 1;
        memcpy(num_buf, l, len);
        num_buf[len] = 0;
        COMPLEX(res)[j] = strtoc(num_buf, TRUE);
        break;

      case STRSXP:
        c2 = c;
        if (quoteLen) {
          for (k = 0; k < quoteLen; k++) {
            if (*l == quoteChars[k]) {
              l++;
              if (!(c2 = memchr(l, quoteChars[k], le - l))) {
                Rf_warning("End of line within quoted string!");
                c = c2 = le;
              } else {
                if (!(c = memchr(c2, (unsigned char) sep, le - c2)))
                  c = le;
              }
            }
          }
        }
        SET_STRING_ELT(res, j, Rf_mkCharLen(l, c2 - l));
        break;

      case RAWSXP:
        len = (int) (c - l);
        /* watch for overflow and truncate -- should we warn? */
        if (len > sizeof(num_buf) - 1)
            len = sizeof(num_buf) - 1;
        memcpy(num_buf, l, len);
        num_buf[len] = 0;
        RAW(res)[j] = strtoraw(num_buf);
        break;
      }
      l = c + 1;
      j += nrow;
    }

    /* fill up unused columns with NAs */
    while (j < N) {
      switch (TYPEOF(sWhat)) {
      case LGLSXP:
        LOGICAL(res)[j] = NA_INTEGER;
        break;

      case INTSXP:
        INTEGER(res)[j] = NA_INTEGER;
        break;

      case REALSXP:
        REAL(res)[j] = NA_REAL;
        break;

      case CPLXSXP:
        COMPLEX(res)[j].r = NA_REAL;
        COMPLEX(res)[j].i = NA_REAL;
        break;

      case STRSXP:
        SET_STRING_ELT(res, j, R_NaString);
        break;

      case RAWSXP:
        RAW(res)[j] = (Rbyte) 0;
        break;
      }
      j += nrow;
    }
  }

  UNPROTECT(np);
  return res;
}
