mstrsplit <- function(x, sep="|", nsep=NA, line=1L, strict=TRUE, ncol = NA,
                      type=c("character", "numeric")) {
  type_flag = as.integer(match.arg(type) == "character")
  .Call(mat_split, x, sep, nsep, line, !strict, ncol, type_flag)
}
