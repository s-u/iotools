#include <string.h>

#define USE_RINTERNALS 1

#include <Rinternals.h>

static int sep_len(const char *s, int sep) {
    char *m = strchr(s, sep);
    return (!m) ? strlen(s) : m - s;
}

/* returns the index of the smallest key (in C lexicograph. order) */
SEXP which_min_key(SEXP sStr, SEXP sSep) {
    int len = 0, w = 0, i, n = LENGTH(sStr), nested = TYPEOF(sStr) == VECSXP;
    const char *ref;
    int sep;
    if (TYPEOF(sStr) != STRSXP && TYPEOF(sStr) != VECSXP) Rf_error("keys must be a character vector");
    if (n < 1) return allocVector(INTSXP, 0);
    else if (n == 1) return ScalarInteger(1);
    sep = (TYPEOF(sSep) != STRSXP || LENGTH(sSep) < 1) ? 0 : ((int) (unsigned char) CHAR(STRING_ELT(sSep, 0))[0]);
    if (nested)
	while (w < n && (TYPEOF(VECTOR_ELT(sStr, w)) != STRSXP || LENGTH(VECTOR_ELT(sStr, w)) < 1)) w++;
    else
	while (w < n && STRING_ELT(sStr, w) == R_NaString) w++;
    if (w >= n) return allocVector(INTSXP, 0);
    len = sep_len(ref = CHAR(nested ? STRING_ELT(VECTOR_ELT(sStr, w), 0) : STRING_ELT(sStr, w)), sep);
    for (i = w + 1; i < n; i++) {
	if (nested && (TYPEOF(VECTOR_ELT(sStr, i)) != STRSXP ||
		       LENGTH(VECTOR_ELT(sStr, i)) < 1)) continue;
	if (!nested && STRING_ELT(sStr, i) == R_NaString) continue;
	const char *cand = CHAR(nested ? STRING_ELT(VECTOR_ELT(sStr, i), 0) : STRING_ELT(sStr, i));
	int c_len = sep_len(cand, sep);
	int cmp = memcmp(ref, cand, (len < c_len) ? len : c_len);
	if (cmp > 0 || (cmp == 0 && c_len < len)) {
	    w = i;
	    ref = cand;
	    len = c_len;
	}
    }
    return ScalarInteger(w + 1);
}
