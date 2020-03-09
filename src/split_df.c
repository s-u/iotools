#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "utils.h"

/* from tparse.c (which is based on code form fasttime) */
double parse_ts(const char *c_start, const char *c_end);

SEXP df_split(SEXP s, SEXP sSep, SEXP sNamesSep, SEXP sResilient, SEXP sNcol,
            SEXP sWhat, SEXP sColNames, SEXP sSkip, SEXP sNlines, SEXP sQuote) {
    char sep;
    int nsep, use_ncol, resilient, ncol;
    long i, j, k, m, len, nmsep_flag, skip, quoteLen;
    unsigned long nrow;
    char num_buf[48];
    const char *c, *c2, *sraw = 0, *send = 0, *quoteChars;
    long nlines = asLong(sNlines, -1);

    SEXP sOutput, tmp, sOutputNames, st, clv;

    /* Parse inputs */
    sep = CHAR(STRING_ELT(sSep, 0))[0];
    nsep = (TYPEOF(sNamesSep) == STRSXP && LENGTH(sNamesSep) > 0) ? ((int) (unsigned char) *CHAR(STRING_ELT(sNamesSep, 0))) : -1;

    nmsep_flag = (nsep > 0);
    use_ncol = asInteger(sNcol);
    resilient = asInteger(sResilient);
    ncol = use_ncol; /* NOTE: "character" is prepended by the R code if nmsep is TRUE,
                        so ncol *does* include the key column */
    skip = asLong(sSkip, 0);

    /* parse quote information */
    quoteChars = CHAR(STRING_ELT(sQuote, 0));
    quoteLen = strlen(quoteChars);

    /* count non-NA columns */
    for (i = 0; i < use_ncol; i++)
	if (TYPEOF(VECTOR_ELT(sWhat,i)) == NILSXP) ncol--;

    /* check input */
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
	nrow = XLENGTH(s);
	if (nrow >= skip) {
	    nrow -= skip;
	} else {
	    skip = nrow;
	    nrow = 0;
	}
    } else
	Rf_error("invalid input to split - must be a raw or character vector");

    if (nlines >= 0 && nrow > nlines) nrow = nlines;

    /* allocate result */
    PROTECT(sOutput = allocVector(VECSXP, ncol));

    /* set names */
    setAttrib(sOutput, R_NamesSymbol, sOutputNames = allocVector(STRSXP, ncol));

    if (nrow > INT_MAX)
	Rf_warning("R currently doesn't support large data frames, but we have %lu rows, returning a named list instead", nrow);
    else {
	/* set automatic row names */
	PROTECT(tmp = allocVector(INTSXP, 2));
	INTEGER(tmp)[0] = NA_INTEGER;
	INTEGER(tmp)[1] = -nrow;
	setAttrib(sOutput, R_RowNamesSymbol, tmp);
	UNPROTECT(1);

	/* set class */
	classgets(sOutput, mkString("data.frame"));
    }

    /* Create SEXP for each element of the output */
    j = 0;
    for (i = 0; i < use_ncol; i++) {
      if (TYPEOF(VECTOR_ELT(sWhat,i)) != NILSXP) /* copy col.name */
        SET_STRING_ELT(sOutputNames, j, STRING_ELT(sColNames, i));

      switch (TYPEOF(VECTOR_ELT(sWhat,i))) {
      case LGLSXP:
      case INTSXP:
      case REALSXP:
      case CPLXSXP:
      case STRSXP:
      case RAWSXP:
        SET_VECTOR_ELT(sOutput, j++, allocVector(TYPEOF(VECTOR_ELT(sWhat,i)), nrow));
        break;

      case VECSXP:
        SET_VECTOR_ELT(sOutput, j++, st = allocVector(REALSXP, nrow));
        clv = PROTECT(allocVector(STRSXP, 2));
        SET_STRING_ELT(clv, 0, mkChar("POSIXct"));
        SET_STRING_ELT(clv, 1, mkChar("POSIXt"));
        setAttrib(st, R_ClassSymbol, clv);
        /* this is somewhat a security precaution such that users
           don't get surprised -- if there is no TZ R will
           render it in local time - which is correct but
           may confuse people that didn't use GMT to start with */
        setAttrib(st, install("tzone"), PROTECT(mkString("GMT")));
        UNPROTECT(2);
        break;

      case NILSXP:
        break;

      default:
        Rf_error("Unsupported input to what %u.", TYPEOF(VECTOR_ELT(sWhat,i)));
        break;
      }
    }

    /* Cycle through the rows and extract the data */
    for (k = 0; k < nrow; k++) {
      const char *l = 0, *le;
      if (TYPEOF(s) == RAWSXP) {
          l = sraw;
          le = memchr(l, '\n', send - l);
          if (!le) le = send;
          sraw = le + 1;
          if (*(le - 1) == '\r' ) le--; /* account for DOS-style '\r\n' */
      } else {
          l = CHAR(STRING_ELT(s, k + skip));
          le = l + strlen(l); /* probably lame, but using strings is way inefficient anyway ;) */
      }
      if (nmsep_flag) {
          c = memchr(l, nsep, le - l);
          if (c) {
            SET_STRING_ELT(VECTOR_ELT(sOutput, 0), k, Rf_mkCharLen(l, c - l));
            l = c + 1;
          } else
            SET_STRING_ELT(VECTOR_ELT(sOutput, 0), k, R_BlankString);
      }

      i = nmsep_flag;
      j = nmsep_flag;
      while (l < le) {
        if (!(c = memchr(l, sep, le - l)))
          c = le;

        if (i >= use_ncol) {
          if (resilient) break;
          Rf_error("line %lu: too many input columns (expected %u)", k, use_ncol);
        }

        switch(TYPEOF(VECTOR_ELT(sWhat,i))) { // NOTE: no matching case for NILSXP
        case LGLSXP:
          len = (int) (c - l);
          if (len > sizeof(num_buf) - 1)
              len = sizeof(num_buf) - 1;
          memcpy(num_buf, l, len);
          num_buf[len] = 0;
          int tr = StringTrue(num_buf), fa = StringFalse(num_buf);
          LOGICAL(VECTOR_ELT(sOutput, j))[k] = (tr || fa) ? tr : NA_INTEGER;
          j++;
          break;

        case INTSXP:
          len = (int) (c - l);
          /* watch for overflow and truncate -- should we warn? */
          if (len > sizeof(num_buf) - 1)
              len = sizeof(num_buf) - 1;
          memcpy(num_buf, l, len);
          num_buf[len] = 0;
          INTEGER(VECTOR_ELT(sOutput, j))[k] = Strtoi(num_buf, 10);
          j++;
          break;

        case REALSXP:
          len = (int) (c - l);
          /* watch for overflow and truncate -- should we warn? */
          if (len > sizeof(num_buf) - 1)
              len = sizeof(num_buf) - 1;
          memcpy(num_buf, l, len);
          num_buf[len] = 0;
          REAL(VECTOR_ELT(sOutput, j))[k] = R_atof(num_buf);
          j++;
          break;

        case CPLXSXP:
          len = (int) (c - l);
          /* watch for overflow and truncate -- should we warn? */
          if (len > sizeof(num_buf) - 1)
              len = sizeof(num_buf) - 1;
          memcpy(num_buf, l, len);
          num_buf[len] = 0;
          COMPLEX(VECTOR_ELT(sOutput, j))[k] = strtoc(num_buf, TRUE);
          j++;
          break;

        case STRSXP:
          c2 = c;
          if (quoteLen) {
            for (m = 0; m < quoteLen; m++) {
              if (*l == quoteChars[m]) {
                l++;
                if (!(c2 = memchr(l, quoteChars[m], le - l))) {
                  Rf_error("End of line within quoted string.");
                } else {
                  if (!(c = memchr(c2, (unsigned char) sep, le - c2)))
                    c = le;
                }
              }
            }
          }
          SET_STRING_ELT(VECTOR_ELT(sOutput, j), k, Rf_mkCharLen(l, c2 - l));
          j++;
          break;

        case RAWSXP:
          len = (int) (c - l);
          /* watch for overflow and truncate -- should we warn? */
          if (len > sizeof(num_buf) - 1)
              len = sizeof(num_buf) - 1;
          memcpy(num_buf, l, len);
          num_buf[len] = 0;
          RAW(VECTOR_ELT(sOutput, j))[k] = strtoraw(num_buf);
          j++;
          break;

        case VECSXP:
          REAL(VECTOR_ELT(sOutput, j))[k] = parse_ts(l, c);
          j++;
        }

        l = c + 1;
        i++;
      }

      /* fill-up unused columns */
      while (i < use_ncol) {
          switch (TYPEOF(VECTOR_ELT(sWhat,i))) { // NOTE: no matching case for NILSXP
          case LGLSXP:
            LOGICAL(VECTOR_ELT(sOutput, j++))[k] = NA_INTEGER;
            break;

          case INTSXP:
            INTEGER(VECTOR_ELT(sOutput, j++))[k] = NA_INTEGER;
            break;

          case REALSXP:
          case VECSXP:
            REAL(VECTOR_ELT(sOutput, j++))[k] = NA_REAL;
            break;

          case CPLXSXP:
            COMPLEX(VECTOR_ELT(sOutput, j))[k].r = NA_REAL;
            COMPLEX(VECTOR_ELT(sOutput, j++))[k].i = NA_REAL;
            break;

          case STRSXP:
            SET_STRING_ELT(VECTOR_ELT(sOutput, j++), k, R_NaString);
            break;

          case RAWSXP:
            RAW(VECTOR_ELT(sOutput, j))[k] = (Rbyte) 0;
            break;
          }
          i++;
      }
    }

    UNPROTECT(1); /* sOutput */
    return(sOutput);
}
