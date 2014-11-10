#include <stdlib.h>
#include <R.h> /* for NA_REAL */
#define DIGIT(X) ((X) >= '0' && (X) <= '9')

/* start of each month in seconds */
static const int cml[] = { 0, 0, 2678400, 5097600, 7776000, 10368000, 13046400, 15638400,
			   18316800, 20995200, 23587200, 26265600, 28857600, 31536000 };

double parse_ts(const char *c, const char *e) {
    double ts = NA_REAL;
    if (c < e && DIGIT(*c)) {
	int y = 0, m = 0, d = 0, h = 0, mi = 0;
	while (c < e && DIGIT(*c)) {
	    y = y * 10 + (*c - '0');
	    c++;
	}
	if (y < 100) y += 2000;
	y -= 1970;
	if (y >= 0) {
	    ts = ((int)((y + 1) / 4)) * 86400;
	    ts += y * 31536000;
	    while (c < e && !DIGIT(*c)) c++;
	    if (c < e) {
		while (c < e && DIGIT(*c)) {
		    m = m * 10 + (*c - '0');
		    c++;
		}
		if (m > 0 && m < 13) {
		    ts += cml[m];
		    if (m > 2 && (y & 3) == 2) ts += 86400;
		    while (c < e && !DIGIT(*c)) c++;
		    if (c < e) {
			while (c < e && DIGIT(*c)) { d = d * 10 + (*c - '0'); c++; }
			if (d > 1) ts += (d - 1) * 86400;
			while (c < e && !DIGIT(*c)) c++;
			if (c < e) {
			    while (c < e && DIGIT(*c)) { h = h * 10 + (*c - '0'); c++; }
			    ts += h * 3600;
			    while (c < e && !DIGIT(*c)) c++;
			    if (c < e) {
				while (c < e && DIGIT(*c)) { mi = mi * 10 + (*c - '0'); c++; }
				ts += mi * 60;
				while (c < e && !(DIGIT(*c) || *c == '.')) c++;
				if (c < e)
				    ts += atof(c);
			    }
			}
		    }
		}
	    }
	}
    }
    return ts;
}
