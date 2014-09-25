#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>

#define INTEGER_CD 0
#define NUMERIC_CD 1
#define CHAR_CD    2
#define NA_CD      3

SEXP df_split(SEXP s, SEXP sSep, SEXP sNamesSep, SEXP sResilient, SEXP sNcol, SEXP sColTypesCd) {
  char sep;
  int nsep, use_ncol, resilient, ncol, i, j, k, len, nmsep_flag;
  int * col_types;
  unsigned int nrow;
  char num_buf[48];
  const char *c;
  char buff[20];

  SEXP sOutput;
  SEXP * sOutputElem;
  SEXP sOutputRowNames;
  SEXP sOutputNames;
  SEXP sOutputClass;
  SEXP sIndexSymbol;
  SEXP sZerochar;

  sIndexSymbol = mkString("row.index");
  sZerochar = mkChar("");

  // Parse inputs
  sep = CHAR(STRING_ELT(sSep, 0))[0];
  if (TYPEOF(sNamesSep) == STRSXP && LENGTH(sNamesSep) > 0)
    nsep = (int) (unsigned char) *CHAR(STRING_ELT(sNamesSep, 0));
  else nsep = -1;
  nmsep_flag = (nsep > 0);
  use_ncol = asInteger(sNcol);
  resilient = asInteger(sResilient);
  ncol = use_ncol;
  col_types = INTEGER(sColTypesCd);
  for (i = 0; i < use_ncol; i++) if(col_types[i] == NA_CD) ncol--;
  nrow = LENGTH(s);

  PROTECT(sOutputNames = allocVector(STRSXP, ncol));
  PROTECT(sOutputRowNames = allocVector(STRSXP, nrow));

  // Create standard names and row.names for the output dataframe:
  sOutputElem = malloc(ncol * sizeof(SEXP));
  if (nmsep_flag) {
    sprintf(buff, "rowindex");
    SET_STRING_ELT(sOutputNames, j, mkChar(buff));
  }
  for (j = nmsep_flag; j < ncol; j++) {
    sprintf(buff, "V%d", j+(1-nmsep_flag));
    SET_STRING_ELT(sOutputNames, j, mkChar(buff));
  }
  for (i = 0; i < nrow; i++) {
    sprintf(buff, "%d", i+1);
    SET_STRING_ELT(sOutputRowNames, i, mkChar(buff));
  }

  s = PROTECT(coerceVector(s, STRSXP));

  // Create SEXP for each element of the output:
  PROTECT(sOutput = allocVector(VECSXP, ncol));
  j = 0;
  for (i = 0; i < use_ncol; i++) {
    switch (col_types[i]) {
      case INTEGER_CD:
        PROTECT(sOutputElem[j] = allocVector(INTSXP, nrow));
        break;

      case NUMERIC_CD:
        PROTECT(sOutputElem[j] = allocVector(REALSXP, nrow));
        break;

      case CHAR_CD:
        PROTECT(sOutputElem[j] = allocVector(STRSXP, nrow));
        break;
    }

    if(col_types[i] != NA_CD) {
      SET_VECTOR_ELT(sOutput, j, sOutputElem[j]);
      j++;
    }
  }

  // Cycle through the rows and extract the data
  for (k = 0; k < nrow; k++) {
    const char *l = CHAR(STRING_ELT(s, k));
    if (nmsep_flag) {
      c = strchr(l, nsep);
      if (c) {
        SET_STRING_ELT(sOutputElem[0], k, Rf_mkCharLen(l, c - l));
        l = c + 1;
      } else {
        SET_STRING_ELT(sOutputElem[0], k, sZerochar);
      }
    }

    i = nmsep_flag;
    j = nmsep_flag;
    while ((c = strchr(l, sep))) {
      if (i >= use_ncol - 1) {
        if (resilient) break;
        Rf_error("line %lu: too many input columns (expected %u)", k, use_ncol);
      }
      switch(col_types[i]) { // NOTE: no matching case for NA_CD
        case INTEGER_CD:
          len = (int) (c - l);
          memcpy(num_buf, l, len);
          num_buf[len] = 0;
          INTEGER(sOutputElem[j])[k] = atoi(num_buf);
          j++;
          break;

        case NUMERIC_CD:
          len = (int) (c - l);
          memcpy(num_buf, l, len);
          num_buf[len] = 0;
          REAL(sOutputElem[j])[k] = R_atof(num_buf);
          j++;
          break;

        case CHAR_CD:
          SET_STRING_ELT(sOutputElem[j], k, Rf_mkCharLen(l, c - l));
          j++;
          break;
      }
      l = c + 1;
      i++;
    }
    switch (col_types[i]) { // NOTE: no matching case for NA_CD
      case INTEGER_CD:
        // FIXME: need R_atoi to deal with NA correctly; now just
        //   sets bad values to 0 because there is not equivalent
        //   for integers.
        INTEGER(sOutputElem[j])[k] = atoi(l);
        j++;
        break;

      case NUMERIC_CD:
        REAL(sOutputElem[j])[k] = R_atof(l);
        j++;
        break;

      case CHAR_CD:
        SET_STRING_ELT(sOutputElem[j], k, Rf_mkChar(l));
        j++;
        break;
    }
    i++;
    while(i < use_ncol) {
      switch (col_types[i]) { // NOTE: no matching case for NA_CD
        case INTEGER_CD:
          INTEGER(sOutputElem[j])[k] = NA_INTEGER;
          j++;
          break;

        case NUMERIC_CD:
          REAL(sOutputElem[j])[k] = NA_REAL;
          j++;
          break;

        case CHAR_CD:
          SET_STRING_ELT(sOutputElem[j], k, R_NaString);
          j++;
          break;
      }
      i++;
    }
  }

  // Set attributes of the output for class, row.names, and names:
  sOutputClass = PROTECT(allocVector(STRSXP, 1));
  SET_STRING_ELT(sOutputClass, 0, mkChar("data.frame"));
  classgets(sOutput, sOutputClass);

  setAttrib(sOutput, R_RowNamesSymbol, sOutputRowNames);
  setAttrib(sOutput, R_NamesSymbol, sOutputNames);

  UNPROTECT(5 + ncol);
  free(sOutputElem); // allocated array of SEXPs
  return(sOutput);
}
