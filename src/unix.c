#include <sys/types.h>
#include <pwd.h>
#include <unistd.h>
#include <string.h>

#include <Rinternals.h>

SEXP C_setuser(SEXP sName, SEXP sGID) {
    int gid = asInteger(sGID);
    struct passwd *p;
    if (TYPEOF(sName) == STRSXP) {
	if (LENGTH(sName) != 1)
	    Rf_error("user name must be a string");
	p = getpwnam(CHAR(STRING_ELT(sName, 0)));
	if (!p)
	    Rf_error("user '%s' not found", CHAR(STRING_ELT(sName, 0)));
	if (gid >= 0 && setgid(gid))
	    Rf_error("failed to set gid (%d)", gid);
	else {
	    if (setgid(p->pw_gid))
		Rf_error("failed to set gid (%d) from passwd for user %s", p->pw_gid, CHAR(STRING_ELT(sName, 0)));
	    initgroups(p->pw_name, p->pw_gid);
	}
	if (setuid(p->pw_uid))
	    Rf_error("failed to set uid (%d) from passwd for user %s", p->pw_uid, CHAR(STRING_ELT(sName, 0)));
	return ScalarInteger(p->pw_uid);
    } else {
	int uid = asInteger(sName);
	if (gid >= 0 && setgid(gid)) Rf_error("failed to set gid (%d)", gid);
	if (uid >= 0 && setuid(uid)) Rf_error("failed to set uid (%d)", uid);
    }
    return ScalarLogical(1);
}

#include <Rembedded.h>

SEXP C_setTempDir(SEXP sName) {
    if (TYPEOF(sName) != STRSXP || LENGTH(sName) != 1)
	Rf_error("invalid path");
    R_TempDir = strdup(CHAR(STRING_ELT(sName, 0)));
    return sName;
}
