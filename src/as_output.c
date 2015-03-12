#include <Rinternals.h>
#include <Rdefines.h>
#include <string.h>
#include <stdlib.h>

#define FL_RESILIENT 1 /* do not fail, proceed even if the input has more columns */

#include "utils.h"

SEXP as_output_matrix(SEXP sMat, SEXP sNrow, SEXP sNcol, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag) {
  int nrow = INTEGER(sNrow)[0];
  int ncol = INTEGER(sNcol)[0];
  int len = nrow * ncol;
  int rownamesFlag = INTEGER(sRownamesFlag)[0];
  char sep = CHAR(STRING_ELT(sSep, 0))[0];
  char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
  char lend = '\n';
  SEXPTYPE what = TYPEOF(sMat);
  SEXP sRnames = Rf_getAttrib(sMat, R_DimNamesSymbol);
  if (!isNull(sRnames)) {
    sRnames = VECTOR_ELT(sRnames,0);
  } else {
    sRnames = allocVector(STRSXP, nrow);
  }

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
  int buf_pos = 0;
  int i, j;
  int ssize = 0;

  for (i=0; i < nrow; i++)
  {
    if (rownamesFlag) {
      ssize = LENGTH(STRING_ELT(sRnames, i));
      if (ssize + buf_pos + row_len > buf_len) {
        buf_len = 2*buf_len + ssize + row_len;
        char * tmp = realloc(buf, buf_len);
        if (tmp != NULL) {
          buf = tmp;
        } else Rf_error("out of memory");
      }
      memcpy(buf + buf_pos, CHAR(STRING_ELT(sRnames, i)), ssize);
      buf_pos += ssize;
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
            } else Rf_error("out of memory");
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

  /* Is there a better way to do this? */
  SEXP res = PROTECT(allocVector(RAWSXP, buf_pos));
  for (i=0; i < buf_pos; i++)
  {
    RAW(res)[i] = buf[i];
  }

  free(buf);
  UNPROTECT(1);
  return res;
}

SEXP as_output_dataframe(SEXP sData, SEXP sWhat, SEXP sNrow, SEXP sNcol, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag) {
  int i, j;
  int nrow = INTEGER(sNrow)[0];
  int ncol = INTEGER(sNcol)[0];
  int len = nrow * ncol;
  int rownamesFlag = INTEGER(sRownamesFlag)[0];
  char sep = CHAR(STRING_ELT(sSep, 0))[0];
  char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
  char lend = '\n';
  SEXP sRnames = Rf_getAttrib(sData, R_DimNamesSymbol);
  if (!isNull(sRnames)) {
    sRnames = VECTOR_ELT(sRnames,0);
  } else {
    sRnames = allocVector(STRSXP, nrow);
  }

  int row_len = 0;
  int buf_len = 0;
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
    if (rownamesFlag == 1) {
      ssize = LENGTH(STRING_ELT(sRnames, i));
      if (ssize + buf_pos + row_len > buf_len) {
        buf_len = 2*buf_len + ssize + row_len;
        char * tmp = realloc(buf, buf_len);
        if (tmp != NULL) {
          buf = tmp;
        } else Rf_error("out of memory");
      }
      memcpy(buf + buf_pos, CHAR(STRING_ELT(sRnames, i)), ssize);
      buf_pos += ssize;
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
            } else Rf_error("out of memory");
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

  /* Is there a better way to do this? */
  SEXP res = PROTECT(allocVector(RAWSXP, buf_pos));
  for (i=0; i < buf_pos; i++)
  {
    RAW(res)[i] = buf[i];
  }

  free(buf);
  UNPROTECT(1);
  return res;
}

SEXP as_output_vector(SEXP sVector, SEXP sLen, SEXP sNsep, SEXP sNamesFlag) {
  int len = INTEGER(sLen)[0];
  int namesFlag = INTEGER(sNamesFlag)[0];
  char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
  char lend = '\n';
  SEXPTYPE what = TYPEOF(sVector);
  SEXP sRnames = Rf_getAttrib(sVector, R_NamesSymbol);
  if (isNull(sRnames)) {
    sRnames = allocVector(STRSXP, len);
  }

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
  int i, j;
  int ssize = 0;

  for (i=0; i < len; i++)
  {
    if (namesFlag) {
      ssize = LENGTH(STRING_ELT(sRnames, i));
      if (ssize + buf_pos + row_len > buf_len) {
        buf_len = 2*buf_len + ssize + row_len;
        char * tmp = realloc(buf, buf_len);
        if (tmp != NULL) {
          buf = tmp;
        } else Rf_error("out of memory");
      }
      memcpy(buf + buf_pos, CHAR(STRING_ELT(sRnames, i)), ssize);
      buf_pos += ssize;
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
          } else Rf_error("out of memory");
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

  /* Is there a better way to do this? */
  SEXP res = PROTECT(allocVector(RAWSXP, buf_pos));
  for (i=0; i < buf_pos; i++)
  {
    RAW(res)[i] = buf[i];
  }

  free(buf);
  UNPROTECT(1);
  return res;
}


