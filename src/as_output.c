#define  USE_RINTERNALS 1
#include <Rinternals.h>
#include <Rdefines.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <Rversion.h>
#include <R_ext/Connections.h>

#define FL_RESILIENT 1 /* do not fail, proceed even if the input has more columns */

#include "utils.h"

/* FIXME: use long to simplify dealing with 32-bit vs 64-bit indexing,
   but that doesn't work on Windows where MS has brain-deadly defined
   long to be 32-bit even on 64-bit Windows. */

/* we keep all our cached info in a raw vector with this layout */
typedef struct dybuf_info {
    unsigned long pos, size;
    char *data;
    SEXP tail;
    Rconnection con;
    int fd;
} dybuf_info_t;

#if R_VERSION < R_Version(3,3,0)
/* R before 3.3.0 didn't have R_GetConnection() */
extern Rconnection getConnection(int n);
static Rconnection R_GetConnection(SEXP sConn) { return getConnection(asInteger(sConn)); }
#endif

#define DEFAULT_CONN_BUFFER_SIZE 8388608 /* 8Mb */

/* we support:
   iotools.stdout
   iotools.stderr
   iotools.fd(<fd-integer>)
*/
static int parseFD(SEXP sConn) {
    if (TYPEOF(sConn) != SYMSXP && TYPEOF(sConn) != LANGSXP)
	return 0; /* no sybol or lang = no FD */
    if (sConn == Rf_install("iotools.stdout")) return 1;
    if (sConn == Rf_install("iotools.stderr")) return 2;
    if (TYPEOF(sConn) == LANGSXP && CAR(sConn) == Rf_install("iotools.fd") &&
	TYPEOF(CADR(sConn)) == INTSXP && LENGTH(CADR(sConn)) == 1)
	return INTEGER(CADR(sConn))[0];
    return 0;
}

/* returns 1 if sConn is *either* a connection or direct FD, otherwise 0 */
static int isConnection(SEXP sConn) {
    return (sConn && (inherits(sConn, "connection") || parseFD(sConn))) ? 1 : 0;
}

/* NOTE: retuns a *protected* object */
SEXP dybuf_alloc(unsigned long size, SEXP sConn) {
    SEXP s = PROTECT(allocVector(VECSXP, 2));
    SEXP sR = PROTECT(allocVector(RAWSXP, size));
    SEXP r = SET_VECTOR_ELT(s, 0, list1(sR));
    dybuf_info_t *d = (dybuf_info_t*) RAW(SET_VECTOR_ELT(s, 1, allocVector(RAWSXP, sizeof(dybuf_info_t))));
    d->pos  = 0;
    d->size = size;
    d->tail = r;
    d->data = (char*) RAW(CAR(r));
    d->con  = (sConn && inherits(sConn, "connection")) ? R_GetConnection(sConn) : 0;
    d->fd   = parseFD(sConn);
    UNPROTECT(1); /* sR */
    return s;
}

void dybuf_add(SEXP s, const char *data, unsigned long len) {
    dybuf_info_t *d = (dybuf_info_t*) RAW(VECTOR_ELT(s, 1));
    unsigned long n = (d->pos + len > d->size) ? (d->size - d->pos) : len;
    if (!len) return;
    /* printf("[%lu/%lu] %lu\n", d->pos, d->size, len); */
    if (n) {
	memcpy(d->data + d->pos, data, n);
	d->pos += n;
	if (len == n) return;
	data += n;
	len -= n;
    }
    /* printf("[%lu/%lu] filled, need %lu more", d->pos, d->size, len); */

    /* if the output is connection-based, flush */
    if (d->con) { /* connection */
	long wr;
	/* FIXME: should we try partial sends as well ? */
	if ((wr = R_WriteConnection(d->con, d->data, d->pos)) != d->pos)
	    Rf_error("write failed, expected %lu, got %ld", d->pos, wr);
	d->pos = 0;
	/* if the extra content is substantially big, don't even
	   bother storing it and send right away */
	if (len > (d->size / 2)) {
	    /* FIXME: (actually FIX R): WriteConnection should be using const void* */
	    if ((wr = R_WriteConnection(d->con, (void*) data, len)) != len)
		Rf_error("write failed, expected %lu, got %ld", len, wr);
	} else { /* otherwise copy into the buffer */
	    memcpy(d->data, data, len);
	    d->pos = len;
	}
    } else if (d->fd) { /* raw file descriptor (or socket, really) */
	long wr;
	/* FIXME: should we try partial sends as well ? */
	if ((wr = write(d->fd, (const void*) d->data, d->pos)) != d->pos)
	    Rf_error("write failed, expected %lu, got %ld", d->pos, wr);
	d->pos = 0;
	/* if the extra content is substantially big, don't even
	   bother storing it and send right away */
	if (len > (d->size / 2)) {
	    if ((wr = write(d->fd, (const void*) data, len)) != len)
		Rf_error("write failed, expected %lu, got %ld", len, wr);
	} else { /* otherwise copy into the buffer */
	    memcpy(d->data, data, len);
	    d->pos = len;
	}
    } else { /* need more buffers */
	SEXP nb;
	while (len > d->size) d->size *= 2;
	/* printf(", creating %lu more\n", d->size); */
	d->tail = SETCDR(d->tail, list1(nb = allocVector(RAWSXP, d->size)));
	memcpy(d->data = (char*) RAW(nb), data, len);
	d->pos = len;
    }
}

/* this is just a slightly faster version for single byte adds */
void dybuf_add1(SEXP s, char x) {
    dybuf_info_t *d = (dybuf_info_t*) RAW(VECTOR_ELT(s, 1));
    if (d->pos < d->size) {
	d->data[d->pos++] = x;
	return;
    }
    /* fall back to the regular version if alloc is needed */
    dybuf_add(s, &x, 1);
}

SEXP dybuf_collect(SEXP s) {
    dybuf_info_t *d = (dybuf_info_t*) RAW(VECTOR_ELT(s, 1));
    unsigned long total = 0;
    char *dst;
    SEXP head = VECTOR_ELT(s, 0), res;
    if (d->con) {
	long wr;
	/* FIXME: should we try partial sends as well ? */
	if ((wr = R_WriteConnection(d->con, d->data, d->pos)) != d->pos)
	    Rf_error("write failed, expected %lu, got %ld", d->pos, wr);
	d->pos = 0;
	return R_NilValue;
    } else if (d->fd) {
	long wr;
	/* FIXME: should we try partial sends as well ? */
	if ((wr = write(d->fd, d->data, d->pos)) != d->pos)
	    Rf_error("write failed, expected %lu, got %ld", d->pos, wr);
	d->pos = 0;
	return R_NilValue;
    }
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

    /* unfortunately we cannot set the class, because writeRaw()
       does is.vector() check which fails if the object has
       a class and doesn't dispatch so we can't return TRUE :(
       That is rather stupid, but nothing we can do about,
       so we cannot flag our raw vectors with a class.

       classgets(res, PROTECT(mkString("output")));
    */
    UNPROTECT(2);
    return res;
}

/* those are just rough estimates - we'll resize as needed but this
   will give the right order of magnitude. In fact it's better to
   underestimate than overestimate grossly. */
static int guess_size(SEXPTYPE type) {
    switch (type) {
    case LGLSXP:  return 2;
    case INTSXP:  return 5;
    case REALSXP: return 6;
    case CPLXSXP: return 12;
    case STRSXP:  return 5;
    case RAWSXP:  return 3;
    default:
	Rf_error("Unsupported input to what.");
    }
    return 0; /* unreachable */
}

static void store(SEXP buf, SEXP what, R_xlen_t i) {
    char stbuf[128];
    switch (TYPEOF(what)) {
    case LGLSXP:
	{
	    int v;
	    if ((v = INTEGER(what)[i]) == NA_INTEGER)
		dybuf_add(buf, "NA", 2);
	    else
		dybuf_add1(buf, v ? 'T' : 'F');
	    break;
	}

    case INTSXP:
	{
	    int v;
	    if ((v = INTEGER(what)[i]) == NA_INTEGER)
		dybuf_add(buf, "NA", 2);
	    else {
		v = snprintf(stbuf, sizeof(stbuf), "%d", INTEGER(what)[i]);
		dybuf_add(buf, stbuf, v);
	    }
	    break;
	}

    case REALSXP:
	{
	    double v;
	    if (ISNA((v = REAL(what)[i])))
		dybuf_add(buf, "NA", 2);
	    else {
		int n = snprintf(stbuf, sizeof(stbuf), "%.15g", v);
		dybuf_add(buf, stbuf, n);
	    }
	    break;
	}

    case CPLXSXP:
	{
	    double v;
	    if (ISNA((v = COMPLEX(what)[i].r)))
		dybuf_add(buf, "NA", 2);
	    else {
		int n = snprintf(stbuf, sizeof(stbuf), "%.15g%+.15gi",
				 v, COMPLEX(what)[i].i);
		dybuf_add(buf, stbuf, n);
	    }
	    break;
	}

    case STRSXP:
	{
	    /* FIXME: should we supply an option how to represent NA strings? */
	    if (STRING_ELT(what, i) == R_NaString)
		dybuf_add(buf, "NA", 2);
	    else {
		/* FIXME: should we use a defined encoding? */
		const char *c = CHAR(STRING_ELT(what, i));
		dybuf_add(buf, c, strlen(c));
	    }
	    break;
	}

    case RAWSXP:
	{
	    int n = snprintf(stbuf, sizeof(stbuf), "%02x", RAW(what)[i]);
	    dybuf_add(buf, stbuf, n);
	    break;
	}
    }
}

/* when should we use as.character()? We are careful and convert anything that
   has an explicit class - except for AsIs */
static int requires_as_character(SEXP sWhat) {
    /* first, everything that is not a native scalar vector has to be converted */
    switch (TYPEOF(sWhat)) {
    case LGLSXP:
    case INTSXP:
    case REALSXP:
    case CPLXSXP:
    case STRSXP:
    case RAWSXP:
	break;
    default:
	/* anything NOT listed above is not a native vector, so it
	   has to be converted in all cases */
	return 1;
    }

    /* then we still have to check for class -- famous examples that illustrate
       why we need this are factor and POSIXct */
    SEXP sClass = getAttrib(sWhat, R_ClassSymbol);
    return (sClass != R_NilValue && !inherits(sWhat, "AsIs"));
}

SEXP as_output_matrix(SEXP sMat, SEXP sNrow, SEXP sNcol, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag, SEXP sConn) {
    R_xlen_t nrow = asLong(sNrow, -1);
    R_xlen_t ncol = asLong(sNcol, -1);
    if (nrow < 0 || ncol < 0)
	Rf_error("invalid/missing matrix dimensions");

    int rownamesFlag = (TYPEOF(sRownamesFlag) == STRSXP) ? -1 : asInteger(sRownamesFlag);

    if (TYPEOF(sSep) != STRSXP || LENGTH(sSep) != 1)
	Rf_error("sep must be a single string");
    if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
	Rf_error("nsep must be a single string");

    char sep = CHAR(STRING_ELT(sSep, 0))[0];
    char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
    char lend = '\n';
    SEXPTYPE what = TYPEOF(sMat);
    SEXP sRnames = Rf_getAttrib(sMat, R_DimNamesSymbol);
    sRnames = isNull(sRnames) ? 0 : VECTOR_ELT(sRnames, 0);
    if (TYPEOF(sRownamesFlag) == STRSXP) {
	if (XLENGTH(sRownamesFlag) != nrow)
	    Rf_error("length mismatch between rows (%ld) and keys (%ld)", (long) XLENGTH(sRownamesFlag), (long) nrow);
	sRnames = sRownamesFlag;
    }
    int isConn = isConnection(sConn);

    R_xlen_t row_len = ((R_xlen_t) guess_size(what)) * (R_xlen_t) ncol;

    if (rownamesFlag) row_len += 8;

    SEXP buf = dybuf_alloc(isConn ? DEFAULT_CONN_BUFFER_SIZE : (row_len * nrow), sConn);
    R_xlen_t i, j;

    for (i = 0; i < nrow; i++) {
	if (rownamesFlag) {
	    if (sRnames) {
		const char *c = CHAR(STRING_ELT(sRnames, i));
		dybuf_add(buf, c, strlen(c));
	    }
	    dybuf_add1(buf, nsep);
	}

	for (j = 0; j < ncol; j++) {
	    R_xlen_t pos = j;
	    pos *= nrow;
	    pos += i;
	    if (j) dybuf_add1(buf, sep);
	    store(buf, sMat, pos);
	}
	dybuf_add1(buf, lend);
    }

    SEXP res = dybuf_collect(buf);
    UNPROTECT(1); /* buffer */
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


SEXP as_output_dataframe(SEXP sWhat, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag, SEXP sConn, SEXP sRecycle) {
    unsigned long i, j;
    if (TYPEOF(sWhat) != VECSXP)
	Rf_error("object must be a data.frame");
    unsigned long ncol = XLENGTH(sWhat);
    unsigned long nrow = 0;
    unsigned long row_len = 0;
    if (ncol)
	nrow = XLENGTH(VECTOR_ELT(sWhat, 0));
    /* 1 = use row names (TRUE), 0 = don't use row names (FALSE), -1 = user-supplied row names */
    int rownamesFlag = (TYPEOF(sRownamesFlag) == STRSXP) ? -1 : asInteger(sRownamesFlag);
    if (TYPEOF(sSep) != STRSXP || LENGTH(sSep) != 1)
	Rf_error("sep must be a single string");
    if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
	Rf_error("nsep must be a single string");
    char sep = CHAR(STRING_ELT(sSep, 0))[0];
    char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
    char lend = '\n';
    SEXP sRnames = (TYPEOF(sRownamesFlag) == STRSXP) ? sRownamesFlag : getAttrib0(sWhat, R_RowNamesSymbol);
    int isConn = isConnection(sConn), mod = 0;
    int recycle = (asInteger(sRecycle) > 0) ? 1 : 0;
    SEXP as_character = R_NilValue;
    unsigned long *sizes = 0;
    /* drop automatic row names */
    if (TYPEOF(sRnames) != STRSXP) sRnames = NULL;
    if (rownamesFlag == -1 && !sRnames)
	Rf_error("invalid keys value");
    if (rownamesFlag == -1 && XLENGTH(sRnames) != nrow)
	Rf_error("length mismatch between the number of rows and supplied keys");
    for (j = 0; j < ncol; j++) {
	/* we have to call as.character() for objects with a class
	   since they may require a different representation */
	if (requires_as_character(VECTOR_ELT(sWhat, j))) {
	    /* did we create a modified copy yet? If not, do so */
	    if (!mod) {
		/* shallow copy - we use it only internally so should be ok */
		SEXP sData = PROTECT(allocVector(VECSXP, XLENGTH(sWhat)));
		memcpy(&(VECTOR_ELT(sData, 0)), &(VECTOR_ELT(sWhat, 0)),
		       sizeof(SEXP) * XLENGTH(sWhat));
		sWhat = sData;
		mod = 1;
		as_character = Rf_install("as.character");
	    }
	    SEXP asc = PROTECT(lang2(as_character, VECTOR_ELT(sWhat, j)));
	    SET_VECTOR_ELT(sWhat, j, eval(asc, R_GlobalEnv));
	    UNPROTECT(1);
	}
	row_len += guess_size(TYPEOF(VECTOR_ELT(sWhat, j)));
    }

    if (ncol && recycle) { /* this allows us to support lists directly without requiring for
			      them to be a data frames - in those cases we have to check the length
			      of the columns to determine the longest */
	unsigned long min_len = (unsigned long) XLENGTH(VECTOR_ELT(sWhat, 0));
	for (j = 0; j < ncol; j++) {
	    unsigned long l = 0;
	    SEXP el = VECTOR_ELT(sWhat, j);
	    /* NOTE: we can assume that el must be a scalar vector since anything that isn't
	       will be passed through as.character() */
	    l = (unsigned long) XLENGTH(el);
	    if (l < min_len) min_len = l;
	    if (l > nrow) nrow = l;
	}
	/* if all elements have the smae tlength then we don't need to re-cycle, so treat
	   the list exactly like a data frame */
	if (nrow == min_len)
	    recycle = 0;
	else { /* cache lengths since XLENGTH is actually not a cheap operation */
	    SEXP foo = PROTECT(allocVector(RAWSXP, sizeof(long) * ncol));
	    sizes = (unsigned long*) RAW(foo);
	    for (j = 0; j < ncol; j++)
		sizes[j] = (unsigned long) XLENGTH(VECTOR_ELT(sWhat, j));
	}
    }

    /* FIXME: why is the estimate for row names just 1? */
    if (rownamesFlag == 1) row_len++;
    if (rownamesFlag == -1) guess_size(STRSXP);

    SEXP buf = dybuf_alloc(isConn ? DEFAULT_CONN_BUFFER_SIZE : (row_len * nrow), sConn);

    for (i = 0; i < nrow; i++) {
	if (rownamesFlag) {
	    if (sRnames) {
		const char *c = CHAR(STRING_ELT(sRnames, i));
		dybuf_add(buf, c, strlen(c));
	    } else {
		/* FIXME: use sprintf("%d", i) for automatic row names? */
	    }
	    dybuf_add1(buf, nsep);
	}

	if (recycle) /* slower - we need to use modulo to recycle */
	    /* FIXME: modulo is slow for large vectors, should we just keep
	       separate index for every column? It may be worth measuring
	       the impact ... We are already trying to be smart by avoiding modulo
	       for the two most common cases: full-length vectors and vectors of length 1
	       so this will only impact non-trivial recycling */
	    for (j = 0; j < ncol; j++) {
		store(buf, VECTOR_ELT(sWhat, j), (i < sizes[j]) ? i : ((sizes[j] == 1) ? 0 : (i % sizes[j])));
		if (j < ncol - 1)
		    dybuf_add1(buf, (rownamesFlag == 2 && j == 0) ? nsep : sep);
	    }
	else
	    for (j = 0; j < ncol; j++) {
		store(buf, VECTOR_ELT(sWhat, j), i);
		if (j < ncol - 1)
		    dybuf_add1(buf, (rownamesFlag == 2 && j == 0) ? nsep : sep);
	    }
	dybuf_add1(buf, lend);
    }

    if (recycle) UNPROTECT(1); /* sizes cache */
    if (mod) UNPROTECT(1); /* sData */
    SEXP res = dybuf_collect(buf);
    UNPROTECT(1); /* buffer */
    return res;
}

SEXP as_output_vector(SEXP sVector, SEXP sNsep, SEXP sNamesFlag, SEXP sConn) {
    R_xlen_t len = XLENGTH(sVector), i;
    int key_flag = asInteger(sNamesFlag), mod = 0;
    if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
	Rf_error("nsep must be a single string");
    char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
    char lend = '\n';
    SEXP sRnames = Rf_getAttrib(sVector, R_NamesSymbol);
    if (requires_as_character(sVector)) {
	SEXP as_character = Rf_install("as.character");
	SEXP asc = PROTECT(lang2(as_character, sVector));
	sVector = eval(asc, R_GlobalEnv);
	UNPROTECT(1);
	PROTECT(sVector);
	mod = 1;
	/* since as.character() drops names, we want re-use original names, but that
	   means we have to check if it is actually meaningful. We do NOT perform
	   re-cycling since mismatches are unlikely intentional. */
	if (key_flag && TYPEOF(sRnames) == STRSXP &&
	    (TYPEOF(sVector) != STRSXP || XLENGTH(sVector) != XLENGTH(sRnames))) {
	    Rf_warning("coersion of named object using as.character() yields different length (%ld) than original names (%ld), dropping names", (long) XLENGTH(sVector), (long) XLENGTH(sRnames));
	    sRnames = R_NilValue;
	}
    }
    
    SEXPTYPE what = TYPEOF(sVector);
    int isConn = isConnection(sConn);
    if (isNull(sRnames)) sRnames = 0;

    unsigned long row_len = ((unsigned long) guess_size(what));
    if (key_flag) row_len += 8;

    SEXP buf = dybuf_alloc(isConn ? DEFAULT_CONN_BUFFER_SIZE : row_len, sConn);

    for (i = 0; i < len; i++) {
	if (key_flag) {
	    if (sRnames) {
		const char *c = CHAR(STRING_ELT(sRnames, i));
		dybuf_add(buf, c, strlen(c));
	    }
	    dybuf_add1(buf, nsep);
	}
	store(buf, sVector, i);
	dybuf_add1(buf, lend);
    }
    SEXP res = dybuf_collect(buf);
    UNPROTECT(1 + mod);
    return res;
}
