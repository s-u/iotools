mstrsplit <- function(x, sep="|", nsep=NA, line=1L, quiet=FALSE, ncol = NA,
                      type=c("character", "numeric")) {
  type_flag = as.integer(match.arg(type) == "character")
  .Call(mat_split, x, sep, nsep, line, quiet, ncol, type_flag)
}
