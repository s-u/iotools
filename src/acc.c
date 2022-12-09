/* Simple mutable, accumulating pairlist designed to accumulate
   arguments for a function call. It caches the tail for O(1)
   append complexity.
   It is implemented as an external pointer to allow mutability,
   but the pointer value is ignored.
   It has the (S3) class "callAccumulator".

   (C)2022 Simon Urbanek
   License: GPL-2 | GPL-3
*/

#include <Rinternals.h>

SEXP pl_accumulate(SEXP sColl, SEXP sWhat) {
    if (sColl == R_NilValue) {
	SEXP elt;
	SEXP sInfo = PROTECT(Rf_allocVector(VECSXP, 2));
	sColl = PROTECT(R_MakeExternalPtr(0, R_NilValue, sInfo));
	Rf_setAttrib(sColl, R_ClassSymbol, Rf_mkString("callAccumulator"));
	elt = CONS(sWhat, R_NilValue);
	SET_VECTOR_ELT(sInfo, 0, elt); /* head */
	SET_VECTOR_ELT(sInfo, 1, elt); /* tail */
	UNPROTECT(2);
    } else {
	SEXP sInfo = R_ExternalPtrProtected(sColl);
	SEXP sTail = VECTOR_ELT(sInfo, 1);
	SEXP elt = CONS(sWhat, R_NilValue);
	SETCDR(sTail, elt);
	SET_VECTOR_ELT(sInfo, 1, elt);
    }
    return sColl;
}

SEXP pl_count(SEXP sColl) {
    SEXP sHead;
    size_t n = 0;
    if (sColl == R_NilValue)
	return Rf_ScalarInteger(0);
    if (!Rf_inherits(sColl, "callAccumulator"))
	Rf_error("Invalid accumulator object.");
    sHead = VECTOR_ELT(R_ExternalPtrProtected(sColl), 0);
    while (sHead != R_NilValue) {
	n++;
	sHead = CDR(sHead);
    }
    return (n < 1e9) ? Rf_ScalarInteger(n) : Rf_ScalarReal((double) n);
}

SEXP pl_call(SEXP sFn, SEXP sColl, SEXP rho) {
    SEXP res, sCall;
    if (sColl == R_NilValue)
	sCall = lang1(sFn);
    else if (Rf_inherits(sColl, "callAccumulator")) 
	sCall = LCONS(sFn, VECTOR_ELT(R_ExternalPtrProtected(sColl), 0));
    else
	Rf_error("Invalid accumulator object.");
    PROTECT(sCall);
    res = eval(sCall, rho);
    UNPROTECT(1);
    return res;
}
