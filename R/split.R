mstrsplit <- function(x, sep="|", nsep=NA, line=1L, strict=TRUE, ncol = NA,
                      type=c("character", "numeric")) {
  type_flag = as.integer(match.arg(type) == "character")
  .Call(mat_split, x, sep, nsep, line, !strict, ncol, type_flag)
}

dstrsplit <- function(x, col_types, sep="|", nsep=NA, strict=TRUE) {
  if (!is.na(nsep)) col_types = c("character", col_types);
  ncol = length(col_types)
  col_types_cd = match(col_types, c("integer", "numeric", "character", NA)) - 1L
  if(any(is.na(col_types_cd))) stop("Invalid column types")
  .Call(df_split, x, sep, nsep, !strict, ncol, col_types_cd)
}