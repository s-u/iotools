mstrsplit <- function(x, sep="|", nsep=NA, line=1L, quiet=FALSE, ncol = NA,
                      type=c("character", "numeric")) {
    type = match.arg(type)
    if (type == "numeric") {
      .Call(mat_split_numeric, x, sep, nsep, line, quiet, ncol)
    } else {
      .Call(mat_split, x, sep, nsep, line, quiet, ncol)
    }
  }