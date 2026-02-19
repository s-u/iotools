/* R compatibilty macros - working around re-mapped API points */
#ifndef RCOMPAT_H__
#define RCOMPAT_H__

#include <Rversion.h>

#if (R_VERSION >= R_Version(2,0,0))
/* EXTPTR */
#ifdef  EXTPTR_PTR
#undef  EXTPTR_PTR
#endif
#define EXTPTR_PTR(X) R_ExternalPtrAddr(X)
#ifdef  EXTPTR_PROT
#undef  EXTPTR_PROT
#endif
#define EXTPTR_PROT(X) R_ExternalPtrProtected(X)
#ifdef  EXTPTR_TAG
#undef  EXTPTR_TAG
#endif
#define EXTPTR_TAG(X) R_ExternalPtrTag(X)
/* CLOSXP */
#ifdef  BODY_EXPR
#undef  BODY_EXPR
#endif
#define BODY_EXPR(X) R_ClosureExpr(X)
#endif

#if (R_VERSION < R_Version(3,5,0))
#define DATAPTR_RO(X) ((const void*)DATAPTR(X))
#endif

/* guarantee MAYBE_SHARED() and NO_REFERENCES() */
#if (R_VERSION < R_Version(4,5,0))
/* in 4.5.0+ those may no longer be macros */
#ifndef MAYBE_SHARED
#define MAYBE_SHARED(x) (NAMED(x) > 1)
#endif
#ifndef NO_REFERENCES
#define NO_REFERENCES(x) (NAMED(x) == 0)
#endif
/* this was added in 4.5.0 but not as macro */
#define VECTOR_PTR_RO(X) ((const SEXP*) DATAPTR_RO(X))
#endif /* < 4.5.0 */

/* compatibility for code that didn't switch to the new API */
#ifdef NAMED
#undef NAMED
#endif
#define NAMED(X) (NO_REFERENCES(X) ? 0 : (MAYBE_SHARED(X) ? 2 : 1))

#if (R_VERSION < R_Version(4,0,0))
#ifndef STRING_PTR_RO
#define STRING_PTR_RO(x)((const SEXP *) DATAPTR_RO(x))
#endif
#endif

#if (R_VERSION >= R_Version(4,5,0))
/* CLOSXP - new API in 4.5.0 */
#ifdef BODY
#undef BODY
#endif
#define BODY(X) R_ClosureBody(X)
#ifdef FORMALS
#undef FORMALS
#endif
#define FORMALS(X) R_ClosureFormals(X)
#ifdef CLOENV
#undef CLOENV
#endif
#define CLOENV(X) R_ClosureEnv(X)
#endif /* R 4.5.0 */

/* only exists internally, so should not be defined, but to be future-proof ....*/
#ifndef DATAPTR_RW
#if (R_VERSION >= R_Version(4,5,0))
/* make sure you understand the ramifications before use in R 4.5.0+, it should only be
   used from code that allocated the vector */
#define DATAPTR_RW(X) ((void*)DATAPTR_RO(X))
#define STRING_PTR_RW(X) ((SEXP*)STRING_PTR_RO(X))
#define VECTOR_PTR_RW(X) ((SEXP*)VECTOR_PTR_RO(X))
#else
#define DATAPTR_RW(X) DATAPTR(X)
#define STRING_PTR_RW(X) STRING_PTR(X)
#define VECTOR_PTR_RW(X) VECTOR_PTR(X)

#endif
#endif

#if (R_VERSION >= R_Version(4,6,0))
#define NO_SET_NAMED 1
#define HAVE_RESIZE_VECTOR 1
/* this is a bug in current R-devel, we have to use the functions */
#ifdef MAYBE_SHARED
#undef MAYBE_SHARED
int (MAYBE_SHARED)(SEXP x);
#endif
#ifdef NO_REFERENCES
#undef NO_REFERENCES
int (NO_REFERENCES)(SEXP x);
#endif

#else  /* 4.6.0+ */
/* re-map resizable API */

/* Note: we only use the following two which are reliable since we
         remember what we allocated */
#define R_allocResizableVector(X, L) Rf_allocVector(X, L)
#define R_resizeVector(X, L) SETLENGTH(X, L)

/* the remaining ones are untested */
#if R_VERSION < R_Version(3,0,0)
#define XTRUELENGTH(X) TRUELENGTH(X)
#endif
#if R_VERSION < R_Version(3,5,0)
/* not sure how reliable this is since it ingores the growable bit */
#define IS_GROWABLE(x) (XLENGTH(x) < XTRUELENGTH(x))
#endif
#define R_duplicateAsResizable(X) duplicate(X)
#define R_isResizable(X) IS_GROWABLE(X)
#define R_maxLength(X) XTRUELENGTH(x)
#if R_VERSION < R_Version(4,5,0)
#define HAVE_SETLENGTH 1
#endif
#endif /* !4.6.0+ */

#endif /* RCOMPAT_H__ */
