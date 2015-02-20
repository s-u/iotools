as.output <- function(x, ...) UseMethod("as.output")
as.output.default <- function(x, ...) if (is.null(names(x))) as.character(x) else paste(names(x), as.character(x), sep='\t')
as.output.table <- function(x, ...) paste(names(x), x, sep='\t')
as.output.matrix <- function(x, sep, ...) { 
  if (missing(sep)) {
    key_sep = '|'
    field_sep = '\t'
  } else {
    key_sep = field_sep = sep
  }
  o <- apply(x, 1, paste, collapse=key_sep)
  if (!is.null(rownames(x))) 
    o <- paste(rownames(x), o, sep=field_sep)
  o 
}
as.output.list <- function(x, ...) paste(names(x), sapply(x, function (e) paste(as.character(e), collapse='|')), sep='\t')
as.output.data.frame <- function(x, sep, ...) { 
  if (ncol(x) == 1L) 
    return(as.character(x[,1]))
  if (missing(sep)) {
    key_sep = '|'
    field_sep = '\t'
  } else {
    key_sep = field_sep = sep
  }
  o <- apply(x[,-1,drop=FALSE], 1, paste, collapse=key_sep)
  o <- paste(x[,1], o, sep=field_sep)
  o 
}

