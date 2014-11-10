mstrsplit <- function(x, sep="|", nsep=NA, line=1L, strict=TRUE, ncol = NA,
                      type=c("character", "numeric")) {
  type_flag = as.integer(match.arg(type) == "character")
  .Call(mat_split, x, sep, nsep, line, !strict, ncol, type_flag)
}

dstrsplit <- function(x, col_types, sep="|", nsep=NA, strict=TRUE) {
  if (is.null(col.names <- names(col_types)))
    col.names <- paste0("V", seq_along(col_types))
  if (is.list(col_types))
    col_types <- sapply(col_types, function(o) if (any(is.na(o))) NA else class(o)[1L])
  if (!is.na(nsep)) {
    col_types <- c("character", col_types);
    col.names <- c("rowindex", col.names)
  }
  ncol <- length(col_types)
  col_types_cd = match(col_types, c("integer", "numeric", "character", "POSIXct", NA)) - 1L
  if(any(is.na(col_types_cd))) stop("Invalid column types")
  .Call(df_split, x, sep, nsep, !strict, ncol, col_types_cd, col.names)
}

dstrfw <- function(x, col_types, widths, nsep=NA, strict=TRUE) {
  if (is.null(col.names <- names(col_types)))
    col.names <- paste0("V", seq_along(col_types))
  if (is.list(col_types))
    col_types <- sapply(col_types, function(o) if (any(is.na(o))) NA else class(o)[1L])

  if (length(col_types) != length(widths))
    stop("Column types and column widths must be the same length")

  if (!is.na(nsep)) {
    col_types <- c("character", col_types)
    col.names <- c("rowindex", col.names)
    widths <- c(0L, widths) # Add fake width for row index; makes C code much cleaner
  }
  ncol <- length(col_types)

  col_types_cd = match(col_types, c("integer", "numeric", "character", NA)) - 1L
  if(any(is.na(col_types_cd))) stop("Invalid column types")
  .Call(df_split_fw, x, as.integer(widths), nsep, !strict, ncol,
          col_types_cd, col.names)

}