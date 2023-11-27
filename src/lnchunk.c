#include <stdlib.h>
#include <string.h>

#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#ifndef WIN32
#include <sys/select.h>
#endif

#include <Rinternals.h>
#include <Rversion.h>
#include <R_ext/Connections.h>

#if R_CONNECTIONS_VERSION != 1
#error "Missing or unsupported connection API in R"
#endif

#if R_VERSION < R_Version(3,3,0)
/* R before 3.3.0 didn't have R_GetConnection() */
extern Rconnection getConnection(int n);
static Rconnection R_GetConnection(SEXP sConn) { return getConnection(asInteger(sConn)); }
#endif

typedef struct chunk_read {
    int len, alloc;
    SEXP sConn, cache, tail;
    char keySep;
    long in_cache;
    Rconnection con;
    int  fd;
    char buf[1];
} chunk_read_t;

static void chunk_fin(SEXP ref) {
    chunk_read_t *r = (chunk_read_t*) R_ExternalPtrAddr(ref);
    if (r) {
	if (r->sConn) R_ReleaseObject(r->sConn);
	if (r->cache && r->cache != R_NilValue) R_ReleaseObject(r->cache);
	/* Note: tail is just a pointer in the cache chain = no release */
	free(r);
    }
}

static const char *reader_class = "ChunkReader";

static long last_key_back_(const char *buf, int len, char sep);

SEXP create_chunk_reader(SEXP sConn, SEXP sMaxLine, SEXP sKeySep) {
    int max_line = asInteger(sMaxLine), fd;
    Rconnection con;
    chunk_read_t *r;
    SEXP res;

    if (max_line < 64) Rf_error("invalid max.line (must be at least 64)");
    if (inherits(sConn, "fileDescriptor")) {
	con = 0;
	fd = asInteger(sConn);
    } else if (inherits(sConn, "connection")) {
	con = R_GetConnection(sConn);
	fd = -1;
    } else Rf_error("invalid connection");

    r = (chunk_read_t*) malloc(sizeof(chunk_read_t) + max_line);
    if (!r) Rf_error("Unable to allocate %.3fMb for line buffer", ((double) max_line) / (1024.0*1024.0));
    r->len   = 0;
    r->sConn = sConn;
    r->con   = con;
    r->fd    = fd;
    r->alloc = max_line;
    r->keySep = (TYPEOF(sKeySep) == STRSXP && LENGTH(sKeySep) > 0) ? CHAR(STRING_ELT(sKeySep, 0))[0] : 0;
    r->tail = r->cache = R_NilValue;
    r->in_cache = 0;

    res = PROTECT(R_MakeExternalPtr(r, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(res, chunk_fin, TRUE);
    R_PreserveObject(sConn);
    setAttrib(res, R_ClassSymbol, mkString(reader_class));
    UNPROTECT(1);
    return res;
}

static void flush_cache(chunk_read_t *r, SEXP nv, const Rbyte* src, long len) {
    /* copy all cache into a new vector */
    char *ptr = (char*) RAW(nv);
    SEXP w = r->cache;
    while (w != R_NilValue) {
	if (CAR(w) != R_NilValue) {
	    memcpy(ptr, RAW(CAR(w)), LENGTH(CAR(w)));
	    ptr += LENGTH(CAR(w));
	}
	w = CDR(w);
    }
    /* append extra content (if desired) */
    if (len) memcpy(ptr, src, len);
    r->in_cache = 0;
    SETCDR(r->cache, R_NilValue);
    SETCAR(r->cache, R_NilValue);
    r->tail = r->cache;
}

static SEXP key_process(chunk_read_t *r, SEXP val) {
    int hold = 0;
    SEXP nv;
    PROTECT(val);
    if (!LENGTH(val)) { /* no new content, only flush cache */
	if (!r->in_cache) {
	    UNPROTECT(1);
	    return val; /* nothing in cache, pass-thru */
	}
	UNPROTECT(1); /* replace val with a new allocation for the cache content */
	PROTECT(val = allocVector(RAWSXP, r->in_cache));
	flush_cache(r, val, 0, 0);
	UNPROTECT(1); /* val */
	return val;
    }
    /* new content */
    hold = last_key_back_((const char*) RAW(val), LENGTH(val), r->keySep);
    if (hold) { /* some content that can be passed */
	if (!r->in_cache) { /* no cache to prepend - shorten val and create cache with the rest */
	    if (r->cache == R_NilValue)
		R_PreserveObject(r->cache = r->tail = CONS(R_NilValue, R_NilValue));
	    /* create a cache vector */
	    nv = PROTECT(allocVector(RAWSXP, LENGTH(val) - hold));
	    memcpy(RAW(nv), RAW(val) + hold, LENGTH(nv));
	    r->tail = SETCDR(r->tail, CONS(nv, R_NilValue));
	    r->in_cache = LENGTH(nv);
	    /* shorten val */
	    SETLENGTH(val, hold);
	    UNPROTECT(2); /* nv, val */
	    return val;
	}
	/* has cache - create a new vector for cache and val content */
	nv = PROTECT(allocVector(RAWSXP, hold + r->in_cache));
	flush_cache(r, nv, RAW(val), hold);
	/* only keep the remainder in the cache */
	r->in_cache = LENGTH(val) - hold;
	{
	    SEXP cv = PROTECT(allocVector(RAWSXP, r->in_cache));
	    memcpy(RAW(cv), RAW(val) + hold, LENGTH(cv));
	    SETCDR(r->cache, R_NilValue);
	    SETCAR(r->cache, cv);
	}
	UNPROTECT(3); /* cv, nv, val */
	return nv;
    }
    /* no hold - simply append to cache */
    if (r->cache == R_NilValue)
	R_PreserveObject(r->cache = r->tail = CONS(R_NilValue, R_NilValue));
    r->tail = SETCDR(r->tail, CONS(val, R_NilValue));
    r->in_cache += LENGTH(val);
    UNPROTECT(1);
    return R_NilValue; /* special case - this is not an empty vector but rather saying that we need to read again */
}

SEXP chunk_read(SEXP sReader, SEXP sMaxSize, SEXP sTimeout) {
    SEXP res;
    int max_size = asInteger(sMaxSize), i, n;
    double tout = (sTimeout == R_NilValue) ? R_PosInf : asReal(sTimeout);
    chunk_read_t *r;
    char *c;

    if (!inherits(sReader, reader_class))
	Rf_error("invalid reader");
    r = (chunk_read_t*) R_ExternalPtrAddr(sReader);
    if (!r) Rf_error("invalid NULL reader");
    if (max_size < r->alloc) Rf_error("invalid max.size - must be more than the max. line (%d)", r->alloc);

 retry:
    res = PROTECT(allocVector(RAWSXP, max_size));
    c = (char*) RAW(res);
    if ((i = r->len)) {
	memcpy(c, r->buf, r->len);
	r->len = 0;
    }
    while (i < max_size) {
	if (r->con)
	    n = R_ReadConnection(r->con, c + i, max_size - i);
	else { /* direct read using select */
	    if (R_finite(tout) && tout >= 0.0) { /* read with timeout */
#ifdef WIN32
		Rf_error("Direct I/O using timeouts is not supported on Windows");
#else
		fd_set fds;
		struct timeval tv = { (int) tout, 1000 * (((int) (tout * 1000.0)) % 1000) };
		FD_ZERO(&fds);
		FD_SET(r->fd, &fds);
		n = select(r->fd + 1, &fds, 0, 0, &tv);
		if (!n) {
		    UNPROTECT(1);
		    return R_NilValue;
		}
		if (n < 0)
		    Rf_error("Read error during select (%d): %s", errno, strerror(errno));
#endif
	    }
	    n = read(r->fd, c + i, max_size - i);
	    if (n < 0)
		Rf_error("Read error (%d): %s",  errno, strerror(errno));
	}
	if (n < 1) { /* nothing to read, return all we got so far - this is EOF */
	    SEXP tmp = res;
	    if (r->keySep && r->in_cache) { /* combine the last chunk with the cache in one go */
		res = PROTECT(allocVector(RAWSXP, i + r->in_cache));
		flush_cache(r, res, RAW(tmp), i);
		UNPROTECT(2); /* res(tmp), res */
		return res;
	    }
	    res = allocVector(RAWSXP, i);
	    if (LENGTH(res)) memcpy(RAW(res), RAW(tmp), i);
	    /* no need to post-process, since the cache must be empty otherwise special case above would kick in */
	    UNPROTECT(1);
	    return res;
	}
	i += n;
	n = i;
	/* find the last newline */
	while (--i >= 0)
	    if (c[i] == '\n') break;
	if (i >= 0) { /* found a newline */
	    if (n - i > 1) { /* trailing content (incomplete next line) */
		if (n - i - 1 > r->alloc)
		    Rf_error("line too long (%d, max.line was set to %d for this reader), aborting", n - i - 1, r->alloc);
		r->len = n - i - 1;
		memcpy(r->buf, c + i + 1, r->len);
	    }
	    SETLENGTH(res, i + 1); /* include the newline */
	    if (r->keySep) {
		res = key_process(r, res);
		if (res == R_NilValue) {
		    /* we can't return since the result would be empty even though
		       this is not EOF, so we have to carry on reading the next chunk */
		    UNPROTECT(1);
		    /* this is not the most beautiful construct, but we can't use continue without
		       re-allocating the buffer, so we may as well use goto and avoid code duplication */
		    goto retry;
		}
	    }
	    UNPROTECT(1);
	    return res;
	}
	/* newline not found, try to read some more */
	i = n;
    }
    Rf_error("line too long, it exceeds even max.size");
    /* unreachable */
    return R_NilValue;
}

SEXP chunk_apply(SEXP sReader, SEXP sMaxSize, SEXP sMerge, SEXP sFUN, SEXP rho, SEXP sDots,
		 SEXP sBinary, SEXP sInitial) {
    SEXP head = R_NilValue, tail = R_NilValue, elt, res = 0;
    SEXP sHead = PROTECT(CONS(R_NilValue, R_NilValue));
    int binary = (Rf_asInteger(sBinary) == 0) ? 0 : 1;
    int pc = 1;
    if (sMerge == R_NilValue)
	binary = 0; /* always accumulate if there is no merge function */
    while (LENGTH(elt = chunk_read(sReader, sMaxSize, R_NilValue)) > 0) {
	SEXP val = eval(PROTECT(LCONS(sFUN, PROTECT(CONS(PROTECT(elt), sDots)))), rho);
	UNPROTECT(3);
	if (binary) {
	    if (sInitial != R_NilValue) {
		PROTECT(val);
		res = eval(PROTECT(lang2(sInitial, val)), rho);
		UNPROTECT(2);
		PROTECT(res);
		sInitial = R_NilValue; /* we won't apply it again */
		/* the result stays protected */
		pc++;
	    } else {
		if (!res) {
		    /* this looks silly, but we need to match the assumption
		       that there is a protected result which will be replaced by
		       the call - i.e. res is always protected - even if it
		       is R_NilValue */
		    res = PROTECT(R_NilValue);
		    pc++;
		}
		PROTECT(val);
		res = eval(PROTECT(lang3(sMerge, res, val)), rho);
		UNPROTECT(3); /* includes previously protected res */
		PROTECT(res); /* new res, does not affect pc */
	    }
	} else { /* accumulate */
	    if (head == R_NilValue) {
		/* we use pre-allocated CONS to avoid breaking the protection stack for head allocation */
		SETCAR(sHead, val);
		tail = head = sHead;
	    } else {
		tail = SETCDR(tail, CONS(PROTECT(val), R_NilValue));
		UNPROTECT(1);
	    }
	}
    }
    if (binary) /* for binary merge the result is in res */
	head = res ? res : R_NilValue;
    else {
	if (sMerge != R_NilValue) {
	    head = eval(PROTECT(LCONS(sMerge, head)), rho);
	    pc++;
	}
    }
    if (pc) UNPROTECT(pc);
    return head;
}

SEXP last_key_back(SEXP sRaw, SEXP sKeySep);

SEXP chunk_tapply(SEXP sReader, SEXP sMaxSize, SEXP sMerge, SEXP sSep, SEXP sFUN, SEXP rho, SEXP sDots) {
    SEXP head = R_NilValue, tail = R_NilValue, elt, cache, c_tail;
    long in_cache = 0;
    int pc = 1;
    c_tail = cache = PROTECT(CONS(R_NilValue, R_NilValue));
    while (1) {
	PROTECT(elt = chunk_read(sReader, sMaxSize, R_NilValue));
	if (LENGTH(elt) == 0) { /* EOF */
	    if (CAR(cache) == R_NilValue) { /* any cache left? */
		UNPROTECT(1);
		break; /* no? outa here ... */
	    } else { /* replace elt with the content of the cache */
		UNPROTECT(1);
		{ /* copy all cache into a new vector */
		    char *ptr = (char*) RAW(PROTECT(elt = allocVector(RAWSXP, in_cache)));
		    SEXP w = cache;
		    while (w != R_NilValue) {
			memcpy(ptr, RAW(CAR(w)), LENGTH(CAR(w)));
			ptr += LENGTH(CAR(w));
			w = CDR(w);
		    }
		}
		/* empty cache */
		in_cache = 0;
		SETCDR(cache, R_NilValue);
		SETCAR(cache, R_NilValue);
		c_tail = cache;
	    }
	} else {
	    SEXP sHold = last_key_back(elt, sSep);
	    int hold = INTEGER(sHold)[0];
#if CHUNK_DEBUG
	    Rprintf("hold %d of %d (in-cache=%ld)\n", hold, LENGTH(elt), in_cache);
	    {
		char tmp[128], *tc = tmp;
		memcpy(tmp, RAW(elt) + hold - 16, 32);
		while (*tc) { if (*tc == '\n') *tc = '#'; tc++; }
		tmp[32] = 0;
		Rprintf("   [%s]\n", tmp);
		Rprintf("                    ^\n");
	    }
#endif
	    if (!hold) { /* all the same key -- append to cache */
#if CHUNK_DEBUG
		Rprintf(" - single key chunk\n");
#endif
		c_tail = SETCDR(c_tail, CONS(elt, R_NilValue));
		in_cache += LENGTH(elt);
		UNPROTECT(1); /* elt */
		continue; /* and skip the eval step */
	    }

	    if (CAR(cache) != R_NilValue) { /* anything to merge with ? */
		SEXP nv, w = cache;
		char *ptr;
#if CHUNK_DEBUG
		int total = 0, cid = 0;
#endif
		in_cache += hold; /* ok, have to alloc+copy, unfortunately */
		ptr = (char*) RAW(PROTECT(nv = allocVector(RAWSXP, in_cache)));
		while (w != R_NilValue) {
#if CHUNK_DEBUG
		    Rprintf(" - copying from cache #%d, %d (total %d)\n",
			    ++cid, LENGTH(CAR(w)), total += LENGTH(CAR(w)));
#endif
		    memcpy(ptr, RAW(CAR(w)), LENGTH(CAR(w)));
		    ptr += LENGTH(CAR(w));
		    w = CDR(w);
		}
		memcpy(ptr, RAW(elt), hold);
		w = PROTECT(allocVector(RAWSXP, LENGTH(elt) - hold));
		memcpy(RAW(w), RAW(elt) + hold, LENGTH(elt) - hold);
		SETCAR(cache, w);
		SETCDR(cache, R_NilValue);
		c_tail = cache;
		in_cache = LENGTH(w);
		UNPROTECT(3); /* elt, nv, w */
		elt = nv;
		PROTECT(elt);
	    } else { /* no cache - create one */
		SEXP nv = PROTECT(allocVector(RAWSXP, LENGTH(elt) - hold));
		memcpy(RAW(nv), RAW(elt) + hold, LENGTH(elt) - hold);
		SETCAR(cache, nv);
		SETCDR(cache, R_NilValue);
		/* we can do in-place shortening */
		SETLENGTH(elt, hold);
		c_tail = cache;
		in_cache = LENGTH(nv);
		UNPROTECT(1);
	    }
	}

	{
	    SEXP val = eval(PROTECT(LCONS(sFUN, CONS(elt, sDots))), rho);
	    UNPROTECT(2); /* eval + elt */
	    if (sMerge != R_NilValue) {
		if (head == R_NilValue) {
		    tail = head = PROTECT(CONS(val, R_NilValue));
		    pc++;
		} else
		    tail = SETCDR(tail, CONS(val, R_NilValue));
	    }
	}
    }
    if (sMerge != R_NilValue) {
	head = eval(PROTECT(LCONS(sMerge, head)), rho);
	pc++;
    }
    UNPROTECT(pc);
    return head;
}

SEXP pass(SEXP args) {
    return CDR(args);
}

/* find out the size of the last key chunk
   this is typically used to hold back the chunk associated with the last key
   as we can't tell if it will continue in the next chunk */
SEXP last_key_back(SEXP sRaw, SEXP sKeySep) {
    char sep;
    if (TYPEOF(sKeySep) != STRSXP || LENGTH(sKeySep) < 1) Rf_error("Missing or invalid key separator");
    if (TYPEOF(sRaw) != RAWSXP) Rf_error("invalid object - must be a raw vector");
    sep = *CHAR(STRING_ELT(sKeySep, 0));
    return ScalarInteger(last_key_back_((const char*) RAW(sRaw), LENGTH(sRaw), sep));
}

long last_key_back_(const char *buf, int len, char sep) {
    const char *c, *e, *ln, *key, *keye, *key0, *last, *laste;
    c = (const char*) buf;
    e = c + len;
    ln = e - 1;
    while (ln >= c && *ln == '\n') ln--; /* skip trailing newlines */
    e = ln + 1;
    while (--ln >= c && *ln != '\n') {}
    if (ln < c) /* no newline found */
	return 0;
    /* find the key */
    key0 = key = ln + 1;
    if (!(keye = memchr(key, (unsigned char) sep, e - key)))
	keye = e; /* no separator - take the whole line as a key */
    /* step back and look for more keys */
    last = key0;
    laste = keye;
    while (ln >= c) {
	/* unfortunately there is no memrchr so we have to do it the slow way */
	while (--ln > c && *ln != '\n') {}
	{
	    const char *ckey = ln, *ckeye;
	    if (*ckey == '\n') ckey++;
	    ckeye = memchr(ckey, sep, last - ckey);
	    if (!ckeye) ckeye = last - 1; /* no sep -> take whole line (minus \n) */
	    if ((laste - last) != (ckeye - ckey) || /* check length first */
		memcmp(ckey, last, ckeye - ckey)) break; /* get out if keys don't match */
	    /* keys match - move up last */
	    last = ckey;
	    laste = ckeye;
	}
    }
    return last - c;
}
