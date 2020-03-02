## NOTE: although not in the signature, all methods should support
##       the con=NULL argument which allows direct output into a binary
##       connection if set and return the output object otherwise.
as.output <- function(x, ...) UseMethod("as.output")

as.output.default <- function(x, nsep="\t", keys, namesFlag=TRUE, con=NULL, ...) {
    if (missing(keys)) keys <- !is.null(names(x))
    .Call(as_output_vector, x, nsep, keys, con)
}

as.output.data.frame <- function(x, sep = "|", nsep="\t", keys, con=NULL, ...) {
    if (missing(keys)) {
        ok <- attr(x, "output.keys")
        if (is.character(ok)) ## allow forced keys - needed for non-unique keys
            keys <- ok
        else {
            keys <- (.row_names_info(x) > 0)
            if (nrow(x) == 1 && rownames(x) == "1") keys <- FALSE
        }
    }
    if (ncol(x) == 1L)
       .Call(as_output_vector, x[,1], nsep, keys, con)
    else .Call(as_output_dataframe, x, as.character(sep), as.character(nsep), keys, con, FALSE)
}

as.output.list <- function(x, sep="|", nsep="\t", con=NULL, keys=FALSE, ...)
    .Call(as_output_dataframe, x, as.character(sep), as.character(nsep), keys, con, TRUE)

as.output.matrix <- function(x, sep="|", nsep="\t", keys, con=NULL, ...) {
    if(missing(keys)) keys <- !is.null(rownames(x))
    .Call(as_output_matrix, x, nrow(x), ncol(x), as.character(sep),
          as.character(nsep), keys, con)
}

as.output.table <- function(x, nsep="\t", keys=TRUE, con=NULL, ...)
  .Call(as_output_vector, x, as.character(nsep), keys, con)

as.output.raw <- function(x, con=NULL, ...) {
    if (inherits(con, "connection")) {
        writeBin(x, con)
        NULL
    } else x
}

iotools.stdout <- quote(iotools.stdout)
iotools.stderr <- quote(iotools.stderr)
iotools.fd <- function(fd) bquote(iotools.fd(.(fd)))
