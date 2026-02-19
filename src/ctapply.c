#include <stdlib.h>
#include <string.h>

#define USE_RINTERNALS 1
#include <Rinternals.h>

#include "rcompat.h"

#define MIN_CACHE 128

SEXP ctapply_(SEXP args) {
    SEXP rho, vec, by, fun, mfun, cdv = 0, tmp, acc, tail, sDim, sDimNam, sRowNam = R_NilValue, sColNam = R_NilValue;
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
	/* growable vectors are mostly not worth it, so use clean vectors */
	cdv = PROTECT(Rf_allocVector(TYPEOF(vec), (cdlen = N) * cols));
	/* copy the vector slice */
	if (cols > 1)  {
	    int col;
	    int cskip = 0, srcskip = 0;
	    for (col = 0; col < cols; col++, cskip += N, srcskip += n) {
		if (TYPEOF(vec) == INTSXP) memcpy(INTEGER(cdv) + cskip, INTEGER_RO(vec) + i0 + srcskip, sizeof(int) * N);
		else if (TYPEOF(vec) == REALSXP) memcpy(REAL(cdv) + cskip, REAL_RO(vec) + i0 + srcskip, sizeof(double) * N);
		else if (TYPEOF(vec) == STRSXP) memcpy(STRING_PTR_RW(cdv) + cskip, STRING_PTR_RO(vec) + i0 + srcskip, sizeof(SEXP) * N);
		else if (TYPEOF(vec) == VECSXP) memcpy(VECTOR_PTR_RW(cdv) + cskip, VECTOR_PTR_RO(vec) + i0 + srcskip, sizeof(SEXP) * N);
	    }
	} else {
	    if (TYPEOF(vec) == INTSXP) memcpy(INTEGER(cdv), INTEGER_RO(vec) + i0, sizeof(int) * N);
	    else if (TYPEOF(vec) == REALSXP) memcpy(REAL(cdv), REAL_RO(vec) + i0, sizeof(double) * N);
	    else if (TYPEOF(vec) == STRSXP) memcpy(STRING_PTR_RW(cdv), STRING_PTR_RO(vec) + i0, sizeof(SEXP) * N);
	    else if (TYPEOF(vec) == VECSXP) memcpy(VECTOR_PTR_RW(cdv), VECTOR_PTR_RO(vec) + i0, sizeof(SEXP) * N);
	}
	if (is_mat) {
	    SEXP nDim = PROTECT(allocVector(INTSXP, 2));
	    INTEGER(nDim)[0] = N;
	    INTEGER(nDim)[1] = cols;
	    setAttrib(cdv, R_DimSymbol, nDim);
	    UNPROTECT(1);
	    if (has_rownam) {
		SEXP nDN = PROTECT(allocVector(VECSXP, 2));
		SEXP nRN = SET_VECTOR_ELT(nDN, 0, allocVector(STRSXP, N));
		if (sColNam != R_NilValue) SET_VECTOR_ELT(nDN, 1, sColNam);
		for (int rni = 0; rni < N; rni++)
		    SET_STRING_ELT(nRN, rni, STRING_ELT(sRowNam, rni + i0));
		setAttrib(cdv, R_DimNamesSymbol, nDN);
		UNPROTECT(1);
	    }
	}
	eres = eval(PROTECT(LCONS(fun, CONS(cdv, args))), rho);
	UNPROTECT(2); /* eval arg + cdv */
	/* if the result has NAMED > 1 then we have to duplicate it
	   see ctapply(x, y, identity). It should be uncommon, though
	   since most functions will return newly allocated objects */
	/* Rprintf("NAMED(eres)=%d\n", NAMED(eres)); */
	if (MAYBE_SHARED(eres)) eres = duplicate(eres);
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
