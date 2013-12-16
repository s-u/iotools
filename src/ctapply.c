#include <stdlib.h>
#include <string.h>

#define USE_RINTERNALS 1
#include <Rinternals.h>

#define MIN_CACHE 128

SEXP ctapply_(SEXP args) {
    SEXP rho, vec, by, fun, mfun, cdi = 0, cdv = 0, tmp, acc, tail, sDim, sDimNam, sRowNam = R_NilValue, sColNam = R_NilValue;
    int i = 0, n, cdlen, cols = 1, is_mat = 0, has_rownam = 0;
    
    args = CDR(args);
    rho = CAR(args); args = CDR(args);    
    vec = CAR(args); args = CDR(args);
    by  = CAR(args); args = CDR(args);
    fun = CAR(args); args = CDR(args);
    mfun= CAR(args); args = CDR(args);
    tmp = PROTECT(allocVector(VECSXP, 4));
    acc = 0;
    if (TYPEOF(by) != INTSXP && TYPEOF(by) != REALSXP && TYPEOF(by) != STRSXP)
	Rf_error("INDEX must be either integer, real or character vector");
    if (TYPEOF(vec) != INTSXP && TYPEOF(vec) != REALSXP && TYPEOF(vec) != STRSXP && TYPEOF(vec) != VECSXP)
	Rf_error("X must be either integer, real, character or generic vector (list)");
    
    n = LENGTH(by);
    sDim = getAttrib(vec, R_DimSymbol);
    /* FIXME: matrix support is rudimentary - e.g. we ignore dimnames */
    if (TYPEOF(sDim) == INTSXP && LENGTH(sDim) == 2) {
	int *dim_i = INTEGER(sDim);
        cols = dim_i[1];
	is_mat = 1;
	if (dim_i[0] != n)
	    Rf_error("Number of rows in X does not match the length of INDEX");
	sDimNam = getAttrib(vec, R_DimNamesSymbol);
	if (TYPEOF(sDimNam) == VECSXP) {
	    sRowNam = VECTOR_ELT(sDimNam, 0);
	    sColNam = VECTOR_ELT(sDimNam, 1);
	    if (TYPEOF(sRowNam) == STRSXP && LENGTH(sRowNam) == n)
		has_rownam = 1;
	}
    } else if (n != LENGTH(vec))
	Rf_error("X and INDEX must have the same length");

    while (i < n) {
	int i0 = i, N;
	SEXP eres;
	/* find the contiguous stretch */
	while (++i < n) {
	    if ((TYPEOF(by) == INTSXP && INTEGER(by)[i] != INTEGER(by)[i - 1]) ||
		(TYPEOF(by) == STRSXP && STRING_ELT(by, i) != STRING_ELT(by, i - 1)) ||
		(TYPEOF(by) == REALSXP && REAL(by)[i] != REAL(by)[i - 1]))
		break;
	}
	/* [i0, i - 1] is the interval to run on */
	N = i - i0;
	/* allocate cache for both the vector and index */
	if (!cdi) {
	    /* we have to guarantee named > 0 since we'll be modifying it in-place */
	    SET_NAMED(cdi = SET_VECTOR_ELT(tmp, 0, allocVector(TYPEOF(by), (cdlen = ((N < MIN_CACHE) ? MIN_CACHE : N)))), 1);
	    SET_NAMED(cdv = SET_VECTOR_ELT(tmp, 1, allocVector(TYPEOF(vec), cdlen * cols)), 1);
	} else if (cdlen < N) {
	    SET_NAMED(cdi = SET_VECTOR_ELT(tmp, 0, allocVector(TYPEOF(by), (cdlen = N))), 1);
	    SET_NAMED(cdv = SET_VECTOR_ELT(tmp, 1, allocVector(TYPEOF(vec), cdlen * cols)), 1);
	}
	SETLENGTH(cdi, N);
	/* copy the index slice */
	if (TYPEOF(by) == INTSXP) memcpy(INTEGER(cdi), INTEGER(by) + i0, sizeof(int) * N);
	else if (TYPEOF(by) == REALSXP) memcpy(REAL(cdi), REAL(by) + i0, sizeof(double) * N);
	else if (TYPEOF(by) == STRSXP) memcpy(STRING_PTR(cdi), STRING_PTR(by) + i0, sizeof(SEXP) * N);
	/* copy the vector slice */
	if (cols > 1)  {
	    int col;
	    int cskip = 0, srcskip = 0;
	    SETLENGTH(cdv, N * cols);
	    for (col = 0; col < cols; col++, cskip += N, srcskip += n) {
		if (TYPEOF(vec) == INTSXP) memcpy(INTEGER(cdv) + cskip, INTEGER(vec) + i0 + srcskip, sizeof(int) * N);
		else if (TYPEOF(vec) == REALSXP) memcpy(REAL(cdv) + cskip, REAL(vec) + i0 + srcskip, sizeof(double) * N);
		else if (TYPEOF(vec) == STRSXP) memcpy(STRING_PTR(cdv) + cskip, STRING_PTR(vec) + i0 + srcskip, sizeof(SEXP) * N);
		else if (TYPEOF(vec) == VECSXP) memcpy(VECTOR_PTR(cdv) + cskip, VECTOR_PTR(vec) + i0 + srcskip, sizeof(SEXP) * N);
	    }
	} else {
	    SETLENGTH(cdv, N);
	    if (TYPEOF(vec) == INTSXP) memcpy(INTEGER(cdv), INTEGER(vec) + i0, sizeof(int) * N);
	    else if (TYPEOF(vec) == REALSXP) memcpy(REAL(cdv), REAL(vec) + i0, sizeof(double) * N);
	    else if (TYPEOF(vec) == STRSXP) memcpy(STRING_PTR(cdv), STRING_PTR(vec) + i0, sizeof(SEXP) * N);
	    else if (TYPEOF(vec) == VECSXP) memcpy(VECTOR_PTR(cdv), VECTOR_PTR(vec) + i0, sizeof(SEXP) * N);
	}
	if (is_mat) {
	    /* FIXME: could we re-use the same vector object? */
	    SEXP nDim = PROTECT(allocVector(INTSXP, 2));
	    INTEGER(nDim)[0] = N;
	    INTEGER(nDim)[1] = cols;
	    setAttrib(cdv, R_DimSymbol, nDim);
	    UNPROTECT(1);
	    if (has_rownam) {
		/* FIXME: if those are unused, we could use a cache as well ... */
		SEXP nDN = PROTECT(allocVector(VECSXP, 2));
		SEXP nRN = SET_VECTOR_ELT(nDN, 0, allocVector(STRSXP, N));
		/* FIXME: do we need to duplicate colnams? In theory, yes, but what about practice? */
		if (sColNam != R_NilValue) SET_VECTOR_ELT(nDN, 1, sColNam);
		memcpy(VECTOR_PTR(nRN), VECTOR_PTR(sRowNam) + i0, sizeof(SEXP) * N);
		setAttrib(cdv, R_DimNamesSymbol, nDN);
		UNPROTECT(1);
	    }
	}
	eres = eval(PROTECT(LCONS(fun, CONS(cdv, args))), rho);
	UNPROTECT(1); /* eval arg */
	/* if the result has NAMED > 1 then we have to duplicate it
	   see ctapply(x, y, identity). It should be uncommon, though
	   since most functions will return newly allocated objects

	   FIXME: check NAMED == 1 -- may also be bad if the reference is outside,
	   but then NAMED1 should be duplicated before modification so I think we're safe
	*/
	/* Rprintf("NAMED(eres)=%d\n", NAMED(eres)); */
	if (NAMED(eres) > 1) eres = duplicate(eres);
	PROTECT(eres);
	if (!acc) tail = acc = SET_VECTOR_ELT(tmp, 2, list1(eres));
	else tail = SETCDR(tail, list1(eres));
	{
	    char cbuf[64];
	    const char *name = "";
	    if (TYPEOF(by) == STRSXP) name = CHAR(STRING_ELT(by, i0));
	    else if (TYPEOF(by) == INTSXP) {
		snprintf(cbuf, sizeof(cbuf), "%d", INTEGER(by)[i0]);
		name = cbuf;
	    } else { /* FIXME: this one is not consistent with R ... */
		snprintf(cbuf, sizeof(cbuf), "%g", REAL(by)[i0]);
		name = cbuf;
	    }
	    if (*name) SET_TAG(tail, install(name));
	}
	UNPROTECT(1); /* eres */
    }
    UNPROTECT(1); /* tmp */
    if (!acc) return R_NilValue;
    acc = eval(PROTECT(LCONS(mfun, acc)), rho);
    UNPROTECT(1);
    return acc;
}
