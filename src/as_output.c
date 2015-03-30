#define  USE_RINTERNALS 1
#include <Rinternals.h>
#include <Rdefines.h>
#include <string.h>
#include <stdlib.h>

#define FL_RESILIENT 1 /* do not fail, proceed even if the input has more columns */

#include "utils.h"

/* we keep all our cached info in a raw vector with this layout */
typedef struct dybuf_info {
  unsigned long pos, size;
  SEXP tail;
} dybuf_info_t;

/* NOTE: retuns a *protected* object */
SEXP dybuf_alloc(unsigned long size) {
  SEXP s = PROTECT(allocVector(VECSXP, 2));
  SEXP r = SET_VECTOR_ELT(s, 0, list1(allocVector(RAWSXP, size)));
  dybuf_info_t *d = (dybuf_info_t*) RAW(SET_VECTOR_ELT(s, 1, allocVector(RAWSXP, sizeof(dybuf_info_t))));
  d->pos  = 0;
  d->size = size;
  d->tail = r;
  return s;
}

void dybuf_add(SEXP s, const char *data, unsigned long len) {
  dybuf_info_t *d = (dybuf_info_t*) RAW(VECTOR_ELT(s, 1));
  unsigned long n = (d->pos + len > d->size) ? (d->size - d->pos) : len;
  if (!len) return;
  if (n) {
    memcpy(RAW(CAR(d->tail)) + d->pos, data, n);
    d->pos += n;
    if (len == n) return;
    data += n;
    len -= n;
  }
  /* need more buffers */
  {
    SEXP nb;
    /* FIXME: we mostly assume that individual buffers are
       not long vectors so we should guard against that */
    while (len > d->size) len *= 2;
    d->tail = SETCDR(d->tail, list1(nb = allocVector(RAWSXP, d->size)));
    memcpy(RAW(nb), data, len);
    d->pos = len;
  }
}

SEXP dybuf_collect(SEXP s) {
  dybuf_info_t *d = (dybuf_info_t*) RAW(VECTOR_ELT(s, 1));
  unsigned long total = 0;
  char *dst;
  SEXP head = VECTOR_ELT(s, 0), res;
  while (d->tail != head) {
    total += LENGTH(CAR(head));
    head = CDR(head);
  }
  total += d->pos;
  dst = (char*) RAW(res = PROTECT(allocVector(RAWSXP, total)));
  head = VECTOR_ELT(s, 0);
  while (d->tail != head) {
    int l = LENGTH(CAR(head));
    memcpy(dst, RAW(CAR(head)), l);
    dst += l;
    head = CDR(head);
  }
  if (d->pos) memcpy(dst, RAW(CAR(head)), d->pos);
  UNPROTECT(res);
  return res;
}


/* FIXME: all the code below breaks on 32-bit overflows - we need to re-write it
   both with long vector support and 64-bit accumulators

   FIXME: the functions use realloc instead of buffer chains, that will result in
   memory fragmentation; also it uses malloc() instead of R memory pools */

SEXP as_output_matrix(SEXP sMat, SEXP sNrow, SEXP sNcol, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag) {
  int nrow = asInteger(sNrow);
  int ncol = asInteger(sNcol);
  int rownamesFlag = asInteger(sRownamesFlag);
  if (TYPEOF(sSep) != STRSXP || LENGTH(sSep) != 1)
    Rf_error("sep must be a single string");
  char sep = CHAR(STRING_ELT(sSep, 0))[0];
  if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
    Rf_error("nsep must be a single string");
  char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
  char lend = '\n';
  SEXPTYPE what = TYPEOF(sMat);
  SEXP sRnames = Rf_getAttrib(sMat, R_DimNamesSymbol);
  sRnames = isNull(sRnames) ? 0 : VECTOR_ELT(sRnames,0);

  int row_len = 0;
  int buf_len = 0;
  switch (what) {
    case LGLSXP:
      row_len += 2*ncol;
      break;

    case INTSXP:
      row_len += 11*ncol;
      break;

    case REALSXP:
      row_len += 23*ncol;
      break;

    case CPLXSXP:
      row_len += 48*ncol;
      break;

    case STRSXP:
      row_len += 1;
      break;

    case RAWSXP:
      row_len += 3*ncol;
      break;

    default:
      Rf_error("Unsupported input to what.");
      break;
  }
  if (rownamesFlag) row_len++;
  buf_len = row_len*nrow+1;

  char * buf = (char *) malloc(buf_len);
  if (!buf) Rf_error("out of memory");
  int buf_pos = 0;
  int i, j;
  int ssize = 0;

  for (i=0; i < nrow; i++)
  {
    if (rownamesFlag) {
      if (sRnames) {
	ssize = LENGTH(STRING_ELT(sRnames, i));
	if (ssize + buf_pos + row_len > buf_len) {
	  buf_len = 2*buf_len + ssize + row_len;
	  char * tmp = realloc(buf, buf_len);
	  if (tmp != NULL) {
	    buf = tmp;
	  } else {
	    free(buf);
	    Rf_error("out of memory");
	  }
	}
	memcpy(buf + buf_pos, CHAR(STRING_ELT(sRnames, i)), ssize);
	buf_pos += ssize;
      }
      buf[buf_pos] = nsep;
      buf_pos++;
    }

    for (j = 0; j < ncol; j++) {
      switch (what) {
        case LGLSXP:
          if (INTEGER(sMat)[i + j*nrow] == NA_INTEGER)
          {
            buf_pos += snprintf(buf + buf_pos, 2, "%c%c", 'N', 'A');
          } else if (INTEGER(sMat)[i + j*nrow] == 0) {
            buf_pos += snprintf(buf + buf_pos, 2, "%c", 'T');
          } else {
            buf_pos += snprintf(buf + buf_pos, 2, "%c", 'F');
          }
          break;

        case INTSXP:
          if (INTEGER(sMat)[i + j*nrow] == NA_INTEGER)
          {
            buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
          } else {
            buf_pos += snprintf(buf + buf_pos, 11, "%d", INTEGER(sMat)[i + j*nrow]);
          }
          break;

        case REALSXP:
          if (ISNA(REAL(sMat)[i + j*nrow]))
          {
            buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
          } else {
            buf_pos += snprintf(buf + buf_pos, 23, "%.15g", REAL(sMat)[i + j*nrow]);
          }
          break;

        case CPLXSXP:
          if (ISNA(COMPLEX(sMat)[i + j*nrow].r))
          {
            buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
          } else {
            buf_pos += snprintf(buf + buf_pos, 48, "%.15g+%.15gi",
                                  COMPLEX(sMat)[i + j*nrow].r,
                                  COMPLEX(sMat)[i + j*nrow].i);
          }
          break;

        case STRSXP:
          ssize = LENGTH(STRING_ELT(sMat, i + j*nrow));
          if (ssize + buf_pos + row_len > buf_len) {
            buf_len = 2*buf_len + ssize + row_len;
            char * tmp = realloc(buf, buf_len);
            if (tmp != NULL) {
              buf = tmp;
            } else {
	      free(buf);
	      Rf_error("out of memory");
	    }
          }
          memcpy(buf + buf_pos, CHAR(STRING_ELT(sMat, i + j*nrow)), ssize);
          buf_pos += ssize;
          break;

        case RAWSXP:
          buf_pos += snprintf(buf + buf_pos, 3, "%2.2x", RAW(sMat)[i + j*nrow]);
          break;
      }
      buf[buf_pos] = sep;
      buf_pos++;
    }
    buf[buf_pos-1] = lend;
  }

  SEXP res = PROTECT(allocVector(RAWSXP, buf_pos));
  memcpy(RAW(res), buf, buf_pos);

  free(buf);
  UNPROTECT(1);
  return res;
}

/* getAttrib() is broken when trying to access R_RowNamesSymbol
   in more recent R versions so we have to work around that ourselves */
static SEXP getAttrib0(SEXP vec, SEXP name) {
  SEXP s;
  for (s = ATTRIB(vec); s != R_NilValue; s = CDR(s))
    if (TAG(s) == name) return CAR(s);
  return R_NilValue;
}

SEXP as_output_dataframe(SEXP sData, SEXP sWhat, SEXP sNrow, SEXP sNcol, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag) {
  int i, j;
  int nrow = asInteger(sNrow);
  int ncol = asInteger(sNcol);
  int rownamesFlag = asInteger(sRownamesFlag);
  if (TYPEOF(sSep) != STRSXP || LENGTH(sSep) != 1)
    Rf_error("sep must be a single string");
  if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
    Rf_error("nsep must be a single string");
  char sep = CHAR(STRING_ELT(sSep, 0))[0];
  char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
  char lend = '\n';
  SEXP sRnames = getAttrib0(sData, R_RowNamesSymbol);
  int row_len = 0;
  int buf_len = 0;
  if (TYPEOF(sRnames) != STRSXP) sRnames = NULL;

  for (j = 0; j < ncol; j++) {
    switch (TYPEOF(VECTOR_ELT(sWhat,j))) {
      case LGLSXP:
        row_len += 2;
        break;

      case INTSXP:
        row_len += 11;
        break;

      case REALSXP:
        row_len += 23;
        break;

      case CPLXSXP:
        row_len += 48;
        break;

      case STRSXP:
        row_len += 1;
        break;

      case RAWSXP:
        row_len += 3;
        break;

      default:
        Rf_error("Unsupported input to what.");
        break;
    }
  }

  if (rownamesFlag == 1) row_len++;
  buf_len = row_len*nrow+1;

  char * buf = (char *) malloc(buf_len);
  int buf_pos = 0;
  int ssize = 0;

  for (i=0; i < nrow; i++)
  {
    if (rownamesFlag) {
      if (sRnames) {
	ssize = LENGTH(STRING_ELT(sRnames, i));
	if (ssize + buf_pos + row_len > buf_len) {
	  buf_len = 2*buf_len + ssize + row_len;
	  char * tmp = realloc(buf, buf_len);
	  if (tmp != NULL) {
	    buf = tmp;
	  } else {
	    free(buf);
	    Rf_error("out of memory");
	  }
	}
	memcpy(buf + buf_pos, CHAR(STRING_ELT(sRnames, i)), ssize);
	buf_pos += ssize;
      } else {
	/* FIXME: use sprintf("%d", i) for automatic row names? */
      }
      buf[buf_pos] = nsep;
      buf_pos++;
    }

    for (j = 0; j < ncol; j++) {
      switch (TYPEOF(VECTOR_ELT(sWhat,j))) {
        case LGLSXP:
          if (INTEGER(VECTOR_ELT(sData,j))[i] == NA_INTEGER)
          {
            buf_pos += snprintf(buf + buf_pos, 2, "%c%c", 'N', 'A');
          } else if (INTEGER(VECTOR_ELT(sData,j))[i] == 0) {
            buf_pos += snprintf(buf + buf_pos, 2, "%c", 'T');
          } else {
            buf_pos += snprintf(buf + buf_pos, 2, "%c", 'F');
          }
          break;

        case INTSXP:
          if (INTEGER(VECTOR_ELT(sData,j))[i] == NA_INTEGER)
          {
            buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
          } else {
            buf_pos += snprintf(buf + buf_pos, 11, "%d", INTEGER(VECTOR_ELT(sData,j))[i]);
          }
          break;

        case REALSXP:
          if (ISNA(REAL(VECTOR_ELT(sData,j))[i]))
          {
            buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
          } else {
            buf_pos += snprintf(buf + buf_pos, 23, "%.15g", REAL(VECTOR_ELT(sData,j))[i]);
          }
          break;

        case CPLXSXP:
          if (ISNA(COMPLEX(VECTOR_ELT(sData,j))[i].r))
          {
            buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
          } else {
            buf_pos += snprintf(buf + buf_pos, 48, "%.15g+%.15gi",
                                  COMPLEX(VECTOR_ELT(sData,j))[i].r,
                                  COMPLEX(VECTOR_ELT(sData,j))[i].i);
          }
          break;

        case STRSXP:
          ssize = LENGTH(STRING_ELT(VECTOR_ELT(sData,j), i));
          if (ssize + buf_pos + row_len > buf_len) {
            buf_len = 2*buf_len + ssize + row_len;
            char * tmp = realloc(buf, buf_len);
            if (tmp != NULL) {
              buf = tmp;
            } else {
	      free(buf);
	      Rf_error("out of memory");
	    }
          }
          memcpy(buf + buf_pos, CHAR(STRING_ELT(VECTOR_ELT(sData,j), i)), ssize);
          buf_pos += ssize;
          break;

        case RAWSXP:
          buf_pos += snprintf(buf + buf_pos, 3, "%2.2x", RAW(VECTOR_ELT(sData,j))[i]);
          break;
      }
      buf[buf_pos] = (rownamesFlag == 2 && j == 0) ? nsep : sep;
      buf_pos++;
    }
    buf[buf_pos-1] = lend;
  }

  SEXP res = allocVector(RAWSXP, buf_pos);
  memcpy(RAW(res), buf, buf_pos);
  free(buf);
  return res;
}

SEXP as_output_vector(SEXP sVector, SEXP sLen, SEXP sNsep, SEXP sNamesFlag) {
  int len = INTEGER(sLen)[0];
  int namesFlag = asInteger(sNamesFlag);
  if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
    Rf_error("nsep must be a single string");
  char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
  char lend = '\n';
  SEXPTYPE what = TYPEOF(sVector);
  SEXP sRnames = Rf_getAttrib(sVector, R_NamesSymbol);
  if (isNull(sRnames)) sRnames = 0;

  int row_len = 0;
  int buf_len = 0;
  switch (what) {
    case LGLSXP:
      row_len = 2;
      break;

    case INTSXP:
      row_len = 11;
      break;

    case REALSXP:
      row_len = 23;
      break;

    case CPLXSXP:
      row_len = 48;
      break;

    case STRSXP:
      row_len = 1;
      break;

    case RAWSXP:
      row_len = 3;
      break;

    default:
      Rf_error("Unsupported input to what.");
      break;
  }
  if (namesFlag) row_len++;
  buf_len = row_len*len+1;

  char * buf = (char *) malloc(buf_len);
  int buf_pos = 0;
  int i;
  int ssize = 0;

  for (i=0; i < len; i++)
  {
    if (namesFlag) {
      if (sRnames) {
	ssize = LENGTH(STRING_ELT(sRnames, i));
	if (ssize + buf_pos + row_len > buf_len) {
	  buf_len = 2*buf_len + ssize + row_len;
	  char * tmp = realloc(buf, buf_len);
	  if (tmp != NULL) {
	    buf = tmp;
	  } else {
	    free(buf);
	    Rf_error("out of memory");
	  }
	}
	memcpy(buf + buf_pos, CHAR(STRING_ELT(sRnames, i)), ssize);
	buf_pos += ssize;
      }
      buf[buf_pos] = nsep;
      buf_pos++;
    }

    switch (what) {
      case LGLSXP:
        if (INTEGER(sVector)[i] == NA_INTEGER)
        {
          buf_pos += snprintf(buf + buf_pos, 2, "%c%c", 'N', 'A');
        } else if (INTEGER(sVector)[i] == 0) {
          buf_pos += snprintf(buf + buf_pos, 2, "%c", 'T');
        } else {
          buf_pos += snprintf(buf + buf_pos, 2, "%c", 'F');
        }
        break;

      case INTSXP:
        if (INTEGER(sVector)[i] == NA_INTEGER)
        {
          buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
        } else {
          buf_pos += snprintf(buf + buf_pos, 11, "%d", INTEGER(sVector)[i]);
        }
        break;

      case REALSXP:
        if (ISNA(REAL(sVector)[i]))
        {
          buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
        } else {
          buf_pos += snprintf(buf + buf_pos, 23, "%.15g", REAL(sVector)[i]);
        }
        break;

      case CPLXSXP:
        if (ISNA(COMPLEX(sVector)[i].r))
        {
          buf_pos += snprintf(buf + buf_pos, 3, "%c%c", 'N', 'A');
        } else {
          buf_pos += snprintf(buf + buf_pos, 48, "%.15g+%.15gi",
                                COMPLEX(sVector)[i].r,
                                COMPLEX(sVector)[i].i);
        }
        break;

      case STRSXP:
        ssize = LENGTH(STRING_ELT(sVector, i));
        if (ssize + buf_pos + row_len > buf_len) {
          buf_len = 2*buf_len + ssize + row_len;
          char * tmp = realloc(buf, buf_len);
          if (tmp != NULL) {
            buf = tmp;
          } else {
	    free(buf);
	    Rf_error("out of memory");
	  }
        }
        memcpy(buf + buf_pos, CHAR(STRING_ELT(sVector, i)), ssize);
        buf_pos += ssize;
        break;

      case RAWSXP:
        buf_pos += snprintf(buf + buf_pos, 3, "%2.2x", RAW(sVector)[i]);
        break;
    }

    buf[buf_pos] = lend;
    buf_pos++;
  }

  SEXP res = allocVector(RAWSXP, buf_pos);
  memcpy(RAW(res), buf, buf_pos);

  free(buf);
  UNPROTECT(1);
  return res;
}


