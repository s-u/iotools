as.output <- function(x, ...) UseMethod("as.output")

as.output.default <- function(x, nsep="\t", keys, namesFlag=TRUE,...) {
  if(missing(keys)) keys <- !is.null(names(x))
  if (is.vector(x) && (mode(x) %in% c("numeric","character","integer","logical","complex","raw"))) {
    return(.Call(as_output_vector, x, nsep, keys))
  }
  if (!namesFlag) as.character(x) else paste(names(x), as.character(x), sep=nsep)
}

as.output.data.frame <- function(x, sep = "|", nsep="\t", keys, ...) {
  if(missing(keys)) {
    keys <- (.row_names_info(x) > 0)
    if (nrow(x) == 1 && rownames(x) == "1") keys <- FALSE
  }
  if (ncol(x) == 1L)
    return(as.output.default(x[,1], nsep=nsep, keys=keys, ...))

  colClasses = sapply(x, class)
  known <- colClasses %in% c("logical", "integer", "numeric", "complex", "character", "raw")
  for (j in (1:ncol(x))[!known])
    x[,j] = as.character(x[,j])
  colClasses[!known] = "character"
  what = sapply(colClasses, do.call, list(0))
  .Call(as_output_dataframe, x, what, nrow(x), ncol(x), as.character(sep),
        as.character(nsep), keys)
}

as.output.list <- function(x, sep="|", nsep="\t", ...)
  paste(names(x), sapply(x, function (e) paste(as.character(e), collapse=sep)), sep=nsep)

as.output.matrix <- function(x, sep="|", nsep="\t", keys, ...) {
  if(missing(keys)) keys <- !is.null(rownames(x))
  .Call(as_output_matrix, x, nrow(x), ncol(x), as.character(sep),
        as.character(nsep), keys)
}

as.output.table <- function(x, nsep="\t", keys=TRUE, ...)
  .Call(as_output_vector, x, as.character(nsep), keys)
