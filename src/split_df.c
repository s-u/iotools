#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>

#define INTEGER_CD 0
#define NUMERIC_CD 1
#define CHAR_CD    2
#define NA_CD      3

static long count_lines(SEXP sRaw) {
    const char *c = (const char*) RAW(sRaw);
    const char *e = c + XLENGTH(sRaw);
    long lines = 0;
    while ((c = memchr(c, '\n', e - c))) {
	lines++;
	c++;
    }
    if (e > c && e[-1] != '\n') lines++;
    printf("found %ld lines\n", lines);
    return lines;
}

SEXP df_split(SEXP s, SEXP sSep, SEXP sNamesSep, SEXP sResilient, SEXP sNcol, SEXP sColTypesCd) {
    char sep;
    int nsep, use_ncol, resilient, ncol, i, j, k, len, nmsep_flag;
    int * col_types;
    unsigned int nrow;
    char num_buf[48];
    const char *c, *sraw, *send;
    char buff[20];

    SEXP sOutput, tmp;
    SEXP sOutputNames;
    SEXP sZerochar;

    sZerochar = PROTECT(mkChar(""));
    
    // Parse inputs
    sep = CHAR(STRING_ELT(sSep, 0))[0];
    if (TYPEOF(sNamesSep) == STRSXP && LENGTH(sNamesSep) > 0)
	nsep = (int) (unsigned char) *CHAR(STRING_ELT(sNamesSep, 0));
    else nsep = -1;
    nmsep_flag = (nsep > 0);
    use_ncol = asInteger(sNcol);
    resilient = asInteger(sResilient);
    ncol = use_ncol; /* NOTE: "character" is prepended by the R code if nmsep is TRUE,
			      so ncol *does* include the key column */
    col_types = INTEGER(sColTypesCd);
    
    /* count non-NA columns */
    for (i = 0; i < use_ncol; i++)
	if(col_types[i] == NA_CD) ncol--;

    /* check input */
    if (TYPEOF(s) == RAWSXP) {
	nrow = count_lines(s);
	sraw = (const char*) RAW(s);
	send = sraw + XLENGTH(s);
    } else if (TYPEOF(s) == STRSXP)
	nrow = LENGTH(s);
    else
	Rf_error("invalid input to split - must be a raw or character vector");

    /* allocate result */
    PROTECT(sOutput = allocVector(VECSXP, ncol));

    /* set names */
    setAttrib(sOutput, R_NamesSymbol, sOutputNames = allocVector(STRSXP, ncol));

    /* set automatic row names */
    PROTECT(tmp = allocVector(INTSXP, 2));
    INTEGER(tmp)[0] = NA_INTEGER;
    INTEGER(tmp)[1] = -nrow;
    setAttrib(sOutput, R_RowNamesSymbol, tmp);
    UNPROTECT(1);

    /* set class */
    classgets(sOutput, mkString("data.frame"));
    
    // Create standard names and row.names for the output dataframe:
    if (nmsep_flag) {
	SET_STRING_ELT(sOutputNames, 0, mkChar("rowindex"));
	SET_VECTOR_ELT(sOutput, 0, allocVector(STRSXP, nrow));
    }
  
    for (j = nmsep_flag; j < ncol; j++) {
	snprintf(buff, sizeof(buff), "V%d", j + (1 - nmsep_flag));
	SET_STRING_ELT(sOutputNames, j, mkChar(buff));
    }

    // Create SEXP for each element of the output:
    j = 0;
    for (i = 0; i < use_ncol; i++) {
	switch (col_types[i]) {
	case INTEGER_CD:
	    SET_VECTOR_ELT(sOutput, j++, allocVector(INTSXP, nrow));
	    break;
	    
	case NUMERIC_CD:
	    SET_VECTOR_ELT(sOutput, j++, allocVector(REALSXP, nrow));
	    break;
	    
	case CHAR_CD:
	    SET_VECTOR_ELT(sOutput, j++, allocVector(STRSXP, nrow));
	    break;
	}
    }
    printf("prepared %d columns (expected %d out of %d)\n", j, ncol, use_ncol);

    // Cycle through the rows and extract the data
    for (k = 0; k < nrow; k++) {
	const char *l, *le;
	if (TYPEOF(s) == RAWSXP) {
	    l = sraw;
	    le = memchr(l, '\n', send - l);
	    if (!le) le = send;
	    sraw = le + 1;
	} else {
	    l = CHAR(STRING_ELT(s, k));
	    le = l + strlen(l); /* probably lame, but using strings is way inefficeint anyway ;) */
	}
	if (nmsep_flag) {
	    c = memchr(l, nsep, le - l);
	    if (c) {
		SET_STRING_ELT(VECTOR_ELT(sOutput, 0), k, Rf_mkCharLen(l, c - l));
		l = c + 1;
	    } else
		SET_STRING_ELT(VECTOR_ELT(sOutput, 0), k, sZerochar);
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

	    switch(col_types[i]) { // NOTE: no matching case for NA_CD
	    case INTEGER_CD:
		INTEGER(VECTOR_ELT(sOutput, j))[k] = atoi(l);
		j++;
		break;
		
	    case NUMERIC_CD:
		len = (int) (c - l);
		/* watch for overflow and truncate -- should we warn? */
		if (len > sizeof(num_buf) - 1)
		    len = sizeof(num_buf) - 1;
		memcpy(num_buf, l, len);
		num_buf[len] = 0;
		REAL(VECTOR_ELT(sOutput, j))[k] = R_atof(num_buf);
		j++;
		break;

	    case CHAR_CD:
		SET_STRING_ELT(VECTOR_ELT(sOutput, j), k, Rf_mkCharLen(l, c - l));
		j++;
		break;
	    }

	    l = c + 1;
	    i++;
	}

	/* fill-up unused columns */
	while (i < use_ncol) {
	    switch (col_types[i]) { // NOTE: no matching case for NA_CD
	    case INTEGER_CD:
		INTEGER(VECTOR_ELT(sOutput, j++))[k] = NA_INTEGER;
		break;
		
	    case NUMERIC_CD:
		REAL(VECTOR_ELT(sOutput, j++))[k] = NA_REAL;
		break;
		
	    case CHAR_CD:
		SET_STRING_ELT(VECTOR_ELT(sOutput, j++), k, R_NaString);
		break;
	    }
	    i++;
	}
    }

    UNPROTECT(2); /* sOutput and Zerochar */
    return(sOutput);
}
