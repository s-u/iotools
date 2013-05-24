#include <stdlib.h>
#include <string.h>

#define USE_RINTERNALS 1
#include <Rinternals.h>

#define MIN_CACHE 128

SEXP ctapply(SEXP args) {
    SEXP rho, vec, by, fun, cdi = 0, cdv = 0, tmp, acc;
    int i = 0, n, cdlen;
    
    args = CDR(args);
    rho = CAR(args); args = CDR(args);    
    vec = CAR(args); args = CDR(args);
    by  = CAR(args); args = CDR(args);
    fun = CAR(args); args = CDR(args);
    tmp = PROTECT(allocVector(VECSXP, 3));
    acc = 0;
    if (TYPEOF(by) != INTSXP && TYPEOF(by) != REALSXP && TYPEOF(by) != STRSXP)
	Rf_error("INDEX must be either integer, real or character vector");
    if (TYPEOF(vec) != INTSXP && TYPEOF(vec) != REALSXP && TYPEOF(vec) != STRSXP && TYPEOF(vec) != VECSXP)
	Rf_error("X must be either integer, real, character or generic vector (list)");
    
    if ((n = LENGTH(vec)) != LENGTH(by)) Rf_error("X and INDEX must have the same length");
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
	    cdi = SET_VECTOR_ELT(tmp, 0, allocVector(TYPEOF(by), (cdlen = ((N < MIN_CACHE) ? MIN_CACHE : N))));
	    cdv = SET_VECTOR_ELT(tmp, 1, allocVector(TYPEOF(vec), cdlen));
	} else if (cdlen < N) {
	    cdi = SET_VECTOR_ELT(tmp, 0, allocVector(TYPEOF(by), (cdlen = N)));
	    cdv = SET_VECTOR_ELT(tmp, 1, allocVector(TYPEOF(vec), cdlen));
	}
	SETLENGTH(cdi, N);
	SETLENGTH(cdv, N);
	/* copy the index slice */
	if (TYPEOF(by) == INTSXP) memcpy(INTEGER(cdi), INTEGER(by) + i0, sizeof(int) * N);
	else if (TYPEOF(by) == REALSXP) memcpy(REAL(cdi), REAL(by) + i0, sizeof(double) * N);
	else if (TYPEOF(by) == STRSXP) memcpy(STRING_PTR(cdi), STRING_PTR(by) + i0, sizeof(SEXP) * N);
	/* copy the vector slice */
	if (TYPEOF(vec) == INTSXP) memcpy(INTEGER(cdv), INTEGER(vec) + i0, sizeof(int) * N);
	else if (TYPEOF(vec) == REALSXP) memcpy(REAL(cdv), REAL(vec) + i0, sizeof(double) * N);
	else if (TYPEOF(vec) == STRSXP) memcpy(STRING_PTR(cdv), STRING_PTR(vec) + i0, sizeof(SEXP) * N);
	else if (TYPEOF(vec) == VECSXP) memcpy(VECTOR_PTR(cdv), VECTOR_PTR(vec) + i0, sizeof(SEXP) * N);
	eres = eval(PROTECT(LCONS(fun, CONS(cdv, args))), rho);
	UNPROTECT(1); /* eval arg */
	PROTECT(eres);
	if (!acc) acc = SET_VECTOR_ELT(tmp, 2, list1(eres));
	else acc = SET_VECTOR_ELT(tmp, 2, CONS(eres, acc));
	UNPROTECT(1); /* eres */
    }
    UNPROTECT(1); /* tmp */
    return acc ? acc : R_NilValue;
}
