mstrsplit <- function(x, sep="|", nsep=NA, strict=TRUE, ncol = NA,
                      type=c("character", "numeric", "logical", "integer",  "complex", "raw"),
                      skip=0L, nrows=-1L, quote="") {
  if (is.na(sep)) {
    sep = "\n"
    ncol = 1L
  }
  if (nchar(sep) != 1L) stop("Seperator must be either NA or a one character value.")
  if (length(charToRaw(sep)) != 1L) stop("Seperator must one byte wide (i.e., ASCII).")
  if (!is.na(nsep) && (length(charToRaw(nsep)) != 1L)) stop("Seperator must one byte wide (i.e., ASCII).")
  type = do.call(match.arg(type), list(0))

  if (length(x) == 0L) return(matrix(type,0L,0L))

  .Call(mat_split, x, sep, nsep, !strict, ncol, type, as.integer(skip), as.integer(nrows),
        quote)
}

dstrsplit <- function(x, col_types, sep="|", nsep=NA, strict=TRUE, skip=0L, nrows=-1L, quote="") {
  if (is.na(sep)) {
    sep = "\n"
    ncol = 1L
  }
  if (nchar(sep) != 1L) stop("Seperator must be either NA or a one character value.")
  if (length(charToRaw(sep)) != 1L) stop("Seperator must one byte wide (i.e., ASCII).")
  if (!is.na(nsep) && (length(charToRaw(nsep)) != 1L)) stop("Seperator must one byte wide (i.e., ASCII).")

  if (is.null(col.names <- names(col_types)))
    col.names <- paste0("V", seq_along(col_types))
  if (is.list(col_types))
    col_types <- sapply(col_types, function(o) if (any(is.na(o))) NA else class(o)[1L])
  if (!is.na(nsep)) {
    col_types <- c("character", col_types);
    col.names <- c("rowindex", col.names)
  }

  if (length(x) == 0L)
    return(data.frame(sapply(col_types,do.call,list(0)),stringsAsFactors=FALSE))

  ncol <- length(col_types)
  col_types[col_types %in% c("real", "double")] <- "numeric"
  what <- rep.int(list(""), ncol)
  names(what) <- names(col_types)
  known <- col_types %in% c("logical", "integer", "numeric", "complex", "character", "raw")
  what[known] <- sapply(col_types[known], do.call, list(0))
  what[is.na(col_types)] = list(NULL)
  what[col_types %in% "NULL"] <- list(NULL)
  what[col_types %in% "POSIXct"] <- list(list())

  bad = sapply(what, function(v) length(v) != 0L)
  if (any(bad)) stop(paste0("Invalid input to col_types: ", col_types[bad]))

  res = .Call(df_split, x, sep, nsep, !strict, ncol, what, col.names, as.integer(skip), as.integer(nrows),
             quote)
  return(res)
}

dstrfw <- function(x, col_types, widths, nsep=NA, strict=TRUE, skip=0L, nrows=-1L) {
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

  ncol <- length(col_types)
  col_types[col_types %in% c("real", "double")] <- "numeric"
  what <- rep.int(list(""), ncol)
  names(what) <- names(col_types)
  known <- col_types %in% c("logical", "integer", "numeric", "complex", "character", "raw")
  what[known] <- sapply(col_types[known], do.call, list(0))
  what[is.na(col_types)] = list(NULL)
  what[col_types %in% "NULL"] <- list(NULL)
  what[col_types %in% "POSIXct"] <- list(list())

  bad = sapply(what, function(v) length(v) != 0L)
  if (any(bad)) stop("Invalid input to col_types: ", col_types[bad])

  res = .Call(df_split_fw, x, as.integer(widths), nsep, !strict, ncol,
          what, col.names, as.integer(skip), as.integer(nrows))
  return(res)
}

.default.formatter <- function(x) {
    y <- mstrsplit(x, "|", "\t")
      if (ncol(y) == 1L) y[, 1] else y
}

