#define USE_RINTERNALS 1
#include <Rinternals.h>

#include <string.h>

SEXP C_rbind(SEXP sList) {
    SEXP res, sBase;
    R_xlen_t nrow, ncol, nparts, i, j;

    if (TYPEOF(sList) != VECSXP)
	Rf_error("input must be a list of pieces");
    
    nparts = XLENGTH(sList);
    if (nparts == 0) return R_NilValue; /* FIXME: should we error out? */

    /* use the first element as a template */
    sBase = VECTOR_ELT(sList, 0);
    ncol = XLENGTH(sBase);
    if (ncol < 1)
	Rf_error("input must have at least one column");

    /* sanity check + determine total length */
    nrow = 0;
    for (i = 0; i < nparts; i++) {
	if (TYPEOF(VECTOR_ELT(sList, i)) != VECSXP ||
	    XLENGTH(VECTOR_ELT(sList, i)) != ncol)
	    Rf_error("component %d is not a list/data.frame with %d columns",
		     (int)(i + 1), (int) ncol);
	nrow += XLENGTH(VECTOR_ELT(VECTOR_ELT(sList, i), 0));
    }

    /* allocate everything */
    res = PROTECT(allocVector(VECSXP, ncol));
    for (i = 0; i < ncol; i++)
	SET_VECTOR_ELT(res, i, allocVector(TYPEOF(VECTOR_ELT(sBase, i)), nrow));

    /* copy */
    nrow = 0;
    for (i = 0; i < nparts; i++) {
	R_xlen_t n = XLENGTH(VECTOR_ELT(VECTOR_ELT(sList, i), 0));
	if (n > 0)
	    for (j = 0; j < ncol; j++) {
		SEXP src = VECTOR_ELT(VECTOR_ELT(sList, i), j);
		SEXP dst = VECTOR_ELT(res, j);
		/* FIXME: we could try some coercion ... */
		if (TYPEOF(src) != TYPEOF(dst))
		    Rf_error("part %d, column %d doesn't match the type of the first part",
			     (int)(i + 1), (int)(j + 1));
		/* FIXME: we are not copying attributes - that's intentional
		   since we'd have to worry about their semantics which may
		   require additional processing */
		if (TYPEOF(dst) == REALSXP)
		    memcpy(REAL(dst) + nrow, REAL(src), sizeof(double) * n);
		else if (TYPEOF(dst) == INTSXP)
		    memcpy(INTEGER(dst) + nrow, INTEGER(src), sizeof(int) * n);
		else if (TYPEOF(dst) == STRSXP) {
		    R_xlen_t k;
		    for (k = 0; k < n; k++)
			SET_STRING_ELT(dst, k + nrow, STRING_ELT(src, k));
		} else if (TYPEOF(dst) == VECSXP) {
		    /* no one should be using this - is you have lists as elements
		       then you're aready doomed anyway so we duplicate just to make
		       sure */
		    R_xlen_t k;
		    for (k = 0; k < n; k++)
			SET_VECTOR_ELT(dst, k + nrow, duplicate(VECTOR_ELT(src, k)));
		} else
		    Rf_error("unsupported element type in column %d", (int)(j + 1));
	    }
	nrow += n;
    }
    if (getAttrib(sBase, R_NamesSymbol) != R_NilValue)
	setAttrib(res, R_NamesSymbol, duplicate(getAttrib(sBase, R_NamesSymbol)));

    {
	SEXP sRN = allocVector(INTSXP, 2);
	INTEGER(sRN)[0] = NA_INTEGER;
	INTEGER(sRN)[1] = -nrow; /* FIXME: use REALSXP for large vectors? */
	setAttrib(res, R_RowNamesSymbol, sRN);
    }
    setAttrib(res, R_ClassSymbol, mkString("data.frame"));
    
    UNPROTECT(1);
    return res;
}
