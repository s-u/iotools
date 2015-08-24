#define  USE_RINTERNALS 1
#include <Rinternals.h>
#include <Rdefines.h>
#include <string.h>
#include <stdlib.h>
#include <R_ext/Connections.h>

#define FL_RESILIENT 1 /* do not fail, proceed even if the input has more columns */

#include "utils.h"

/* we keep all our cached info in a raw vector with this layout */
typedef struct dybuf_info {
    unsigned long pos, size;
    char *data;
    SEXP tail;
    Rconnection con;
} dybuf_info_t;

extern Rconnection getConnection(int n);

#define DEFAULT_CONN_BUFFER_SIZE 8388608 /* 8Mb */

/* NOTE: retuns a *protected* object */
SEXP dybuf_alloc(unsigned long size, SEXP sConn) {
    SEXP s = PROTECT(allocVector(VECSXP, 2));
    SEXP r = SET_VECTOR_ELT(s, 0, list1(allocVector(RAWSXP, size)));
    dybuf_info_t *d = (dybuf_info_t*) RAW(SET_VECTOR_ELT(s, 1, allocVector(RAWSXP, sizeof(dybuf_info_t))));
    d->pos  = 0;
    d->size = size;
    d->tail = r;
    d->data = (char*) RAW(CAR(r));
    d->con  = (sConn && inherits(sConn, "connection")) ? getConnection(asInteger(sConn)) : 0;
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
    if (d->con) {
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
    UNPROTECT(1);
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
    char stbuf[64];
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

/* FIXME: all the code below breaks on 32-bit overflows - we need to re-write it
   both with long vector support and 64-bit accumulators */
SEXP as_output_matrix(SEXP sMat, SEXP sNrow, SEXP sNcol, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag, SEXP sConn) {
    int nrow = asInteger(sNrow);
    int ncol = asInteger(sNcol);
    int rownamesFlag = asInteger(sRownamesFlag);

    if (TYPEOF(sSep) != STRSXP || LENGTH(sSep) != 1)
	Rf_error("sep must be a single string");
    if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
	Rf_error("nsep must be a single string");

    char sep = CHAR(STRING_ELT(sSep, 0))[0];
    char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
    char lend = '\n';
    SEXPTYPE what = TYPEOF(sMat);
    SEXP sRnames = Rf_getAttrib(sMat, R_DimNamesSymbol);
    sRnames = isNull(sRnames) ? 0 : VECTOR_ELT(sRnames,0);
    int isConn = inherits(sConn, "connection");

    unsigned long row_len = ((unsigned long) guess_size(what)) * (unsigned long) ncol;

    if (rownamesFlag) row_len += 8;

    SEXP buf = dybuf_alloc(isConn ? DEFAULT_CONN_BUFFER_SIZE : (row_len * nrow), sConn);
    int i, j;

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


/* FIXME: this needs to be converted to using dybuf */
SEXP as_output_dataframe(SEXP sWhat, SEXP sNrow, SEXP sNcol, SEXP sSep, SEXP sNsep, SEXP sRownamesFlag, SEXP sConn) {
    int i, j;
    int nrow = asInteger(sNrow);
    int ncol = asInteger(sNcol);
    int rownamesFlag = asInteger(sRownamesFlag);
    if (TYPEOF(sSep) != STRSXP || LENGTH(sSep) != 1)
	Rf_error("sep must be a single string");
    if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
	Rf_error("nsep must be a single string");
    char sep = CHAR(STRING_ELT(sSep, 0))[0];
    char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
    char lend = '\n';
    SEXP sRnames = getAttrib0(sWhat, R_RowNamesSymbol);
    int row_len = 0;
    int isConn = inherits(sConn, "connection"), mod = 0;
    if (TYPEOF(sRnames) != STRSXP) sRnames = NULL;

    for (j = 0; j < ncol; j++) {
	/* we have to call as.character() for objects with a class
	   since they may require a different representation */
	if (getAttrib(VECTOR_ELT(sWhat, j), R_ClassSymbol) != R_NilValue) {
	    /* did we create a modified copy yet? If not, do so */
	    if (!mod) {
		/* shallow copy - we use it only internally so should be ok */
		SEXP sData = allocVector(VECSXP, XLENGTH(sWhat));
		memcpy(&(VECTOR_ELT(sData, 0)), &(VECTOR_ELT(sWhat, 0)),
		       sizeof(SEXP) * XLENGTH(sWhat));
		sWhat = sData;
		mod = 1;
	    }
	    SEXP asc = PROTECT(lang2(Rf_install("as.character"), VECTOR_ELT(sWhat, j)));
	    SET_VECTOR_ELT(sWhat, j, eval(asc, R_GlobalEnv));
	    UNPROTECT(1);
	}
	row_len += guess_size(TYPEOF(VECTOR_ELT(sWhat, j)));
    }

    if (rownamesFlag == 1) row_len++;

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

	for (j = 0; j < ncol; j++) {
	    store(buf, VECTOR_ELT(sWhat, j), i);
	    if (j < ncol - 1)
		dybuf_add1(buf, (rownamesFlag == 2 && j == 0) ? nsep : sep);
	}
	dybuf_add1(buf, lend);
    }

    SEXP res = dybuf_collect(buf);
    UNPROTECT(1); /* buffer */
    return res;
}

SEXP as_output_vector(SEXP sVector, SEXP sNsep, SEXP sNamesFlag, SEXP sConn) {
    R_xlen_t len = XLENGTH(sVector), i;
    int key_flag = asInteger(sNamesFlag);
    if (TYPEOF(sNsep) != STRSXP || LENGTH(sNsep) != 1)
	Rf_error("nsep must be a single string");
    char nsep = CHAR(STRING_ELT(sNsep, 0))[0];
    char lend = '\n';
    SEXPTYPE what = TYPEOF(sVector);
    SEXP sRnames = Rf_getAttrib(sVector, R_NamesSymbol);
    int isConn = inherits(sConn, "connection");
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
    UNPROTECT(1);
    return res;
}
