as.output <- function(x, ...) UseMethod("as.output")

as.output.default <- function(x, nsep="\t", namesFlag, ...) {
  if(missing(namesFlag)) namesFlag = !is.null(names(x))
  if (is.vector(x) && (mode(x) %in% c("numeric","character","integer","logical","complex","raw"))) {
    return(.Call(as_output_vector, x, length(x), nsep, namesFlag))
  }
  if (!namesFlag) as.character(x) else paste(names(x), as.character(x), sep=nsep)
}

as.output.data.frame <- function(x, sep = "|", nsep="\t", rownamesFlag=TRUE, ...) {
  if (ncol(x) == 1L)
    return(as.output.default(x[,1]))

  colClasses = sapply(x, class)
  known <- colClasses %in% c("logical", "integer", "numeric", "complex", "character", "raw")
  for (j in (1:ncol(x))[!known])
    x[,j] = as.character(x[,j])
  colClasses[!known] = "character"
  what = sapply(colClasses, do.call, list(0))
  .Call(as_output_dataframe, x, what, nrow(x), ncol(x), as.character(sep),
        as.character(nsep), rownamesFlag)
}

as.output.list <- function(x, sep="|", nsep="\t", ...)
  paste(names(x), sapply(x, function (e) paste(as.character(e), collapse=sep)), sep=nsep)

as.output.matrix <- function(x, sep="|", nsep="\t", rownamesFlag, ...) {
  if (missing(rownamesFlag)) rownamesFlag = !is.null(dimnames(x))
  .Call(as_output_matrix, x, nrow(x), ncol(x), as.character(sep),
        as.character(nsep), as.logical(rownamesFlag))
}

as.output.table <- function(x, nsep="\t", namesFlag=TRUE, ...)
  .Call(as_output_vector, x, length(x), as.character(nsep), as.logical(names))
