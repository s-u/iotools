which.min.key <- function(keys, sep="|") .Call(which_min_key, keys, sep)

line.merge <- function(sources, target, sep='|') {
    l <- lapply(sources, readLines, 1L)
    while(length(i <- which.min.key(l, sep))) {
        writeLines(l[[i]], target)
	l[[i]] <- readLines(sources[[i]], 1L)
    }
}
