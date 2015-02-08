# wrappers for using iotools locally; loads input, writes output,

input.file <- function(file_name, formatter = mstrsplit, ...) {
  if (is.character(file_name)) {
    input = file(file_name, "rb")
    on.exit(close(input))
  } else {
    stop("'file_name' must be a character string to a file path.")
  }

  n = file.info(file_name)$size
  formatter(readBin(input, what="raw", n=n), ...)
}

output.file <- function(x, file, formatter.output = NULL) {
  if (is.character(ret <- file)) {
    output = file(file, "wb")
    on.exit(close(output))
  } else if (inherits(file, "connection")) {
    output = file
  } else {
    stop("'file' must be a connection or a character string to a file path.")
  }
  if (is.null(formatter.output)) formatter.output <- as.output

  writeLines(formatter.output(x), output)
  invisible(ret)
}


readAsRaw <- function (con, n, nmax, fileEncoding = "")
{
  if (is.character(con)) {
      if (missing(n)) n = file.info(con)$size
      con = if(nzchar(fileEncoding))
            file(con, "rb", encoding = fileEncoding) else file(con, "rb")
      on.exit(close(con))
  }
  if (!isOpen(con, "rb")) {
      open(con, "rb")
      on.exit(close(con))
  }
  if (missing(n)) n = 65536L
  if (missing(nmax)) nmax = Inf
  mode = summary.connection(con)$mode

  if (mode == "r") {
    ans = raw()
    m = 0
    while(TRUE) {
      this = charToRaw(readChar(con, nchars = min(n, nmax - m), useBytes = TRUE))
      ans = c(ans, this)
      m = m + length(this)
      if ((length(this) < n) || (nmax - m <= 0) ) break
      n = n * 2
    }
  } else if (mode == "rb") {
    ans = raw()
    m = 0
    while(TRUE) {
      this = readBin(con, what="raw", n = min(n, nmax - m), endian = .Platform$endian)
      ans = c(ans, this)
      m = m + length(this)
      if ((length(this) < n) || (nmax - m <= 0) ) break
      n = n * 2
    }
  } else {
    stop("Connection con must be in read mode.")
  }
  return(ans)
}

read.table.raw = function (file, header = FALSE, sep = "", quote = "", dec = ".",
    numerals = c("allow.loss", "warn.loss", "no.loss"), row.names,
    col.names, as.is = TRUE, na.strings = "",
    colClasses = NA, nrows = -1, skip = 0, check.names = TRUE,
    fill = FALSE, strip.white = FALSE, blank.lines.skip = FALSE,
    comment.char = "", allowEscapes = FALSE, flush = FALSE,
    stringsAsFactors = FALSE, fileEncoding = "",
    encoding = "unknown", text, skipNul = FALSE,
    nrowsClasses = 10L, nsep = NA, useScan = NA)
{
  # Read in data as a raw vector:
  if (!missing(file)) {
    r = readAsRaw(file,fileEncoding = fileEncoding)
  } else if (!missing(text)) {
    r = charToRaw(text)
  } else {
    stop('argument "file" is missing, with no default')
  }

  if (missing(row.names)) row.names = NULL
  if (!all(is.na(colClasses))) colClasses[colClasses %in% c("real", "double")] = "numeric"

  # Check if we need to call scan with raw connection instead
  if (sep == "" | quote != "" | allowEscapes | skipNul | !is.null(row.names) |
      dec != "." | !all(as.is) | na.strings != "" | fill | strip.white |
      blank.lines.skip | comment.char != "" | allowEscapes | flush |
      stringsAsFactors | skipNul | !(encoding %in% c("", "unknown", "bytes", "UTF-8"))) {
    if (is.na(useScan)) useScan = TRUE
    if (!useScan) warning("Not able to avoid a call to scan given input parameters.")
  } else if (is.na(useScan)) useScan = FALSE


  # Run a small subset of the data through read.table:
  subset = mstrsplit(r, sep=NA, nsep=nsep, nrows=nrowsClasses, skip=skip)
  if (missing(col.names)) {
    test = read.table(textConnection(subset), header = header, sep = sep, quote = quote,
      dec = dec, numerals = numerals, row.names = row.names, as.is = as.is,
      na.strings = na.strings, colClasses = colClasses, nrows = nrows, skip = skip,
      check.names = check.names, fill = fill, strip.white = strip.white,
      blank.lines.skip = blank.lines.skip, comment.char = comment.char,
      allowEscapes = allowEscapes, flush = flush, stringsAsFactors = stringsAsFactors,
      fileEncoding = fileEncoding, encoding = encoding, skipNul = FALSE)
  } else {
    test = read.table(textConnection(subset), header = header, sep = sep, quote = quote,
      dec = dec, numerals = numerals, row.names = row.names, col.names = col.names, as.is = as.is,
      na.strings = na.strings, colClasses = colClasses, nrows = nrows, skip = skip,
      check.names = check.names, fill = fill, strip.white = strip.white,
      blank.lines.skip = blank.lines.skip, comment.char = comment.char,
      allowEscapes = allowEscapes, flush = flush, stringsAsFactors = stringsAsFactors,
      fileEncoding = fileEncoding, encoding = encoding, skipNul = FALSE)
  }

  if (!all(is.na(colClasses))) {
    colClasses[colClasses != "NULL"] = unlist(lapply(test, function(v) class(v[[1]])[[1]]))
  } else colClasses = unlist(lapply(test,function(v) class(v[[1]])[[1]]))

  index = which(apply(!is.na(test), 2, sum) == 0L)
  if (length(index)) colClasses[colClasses != "NULL"][index] = "character"
  cols = sum(colClasses != "NULL")

  # Set up column classes to use for calls to dstrsplit or scan
  colClassesUse = rep_len("character", cols)
  names(colClassesUse) = names(colClasses)
  known = colClasses %in% c("logical", "integer", "numeric", "complex", "character", "raw")
  colClassesUse[known] = colClasses[known]
  colClassesUse[colClassesUse == "NULL"] = "NULL"
  what = rep.int(list(""), cols)
  what[known] <- sapply(colClassesUse[known], do.call, list(0))
  what[colClasses %in% "NULL"] <- list(NULL)
  keep = !sapply(what, is.null)

  if (useScan) {
    x = scan(rawConnection(r), what = what,
                 sep = sep, quote = quote, dec = dec, nmax = nrows, skip = skip + header,
                 na.strings = na.strings, quiet = TRUE, fill = fill,
                 strip.white = strip.white, blank.lines.skip = blank.lines.skip,
                 multi.line = FALSE, comment.char = comment.char,
                 allowEscapes = allowEscapes, flush = flush, encoding = encoding,
                 skipNul = skipNul)
    nlines = length(x[[which.max(keep)]])
    row.names = .set_row_names(as.integer(nlines))
    x = x[keep]
    class(x) = "data.frame"
    attr(x, "row.names") = row.names
  } else {
    what[colClasses %in% "POSIXct"] <- list(list())
    known[colClasses %in% "POSIXct"] = TRUE
    x = dstrsplit(r, col_types = colClassesUse, sep=sep, nsep=nsep,
                   strict=TRUE, skip=skip + header, nrows=nrows)
  }

  # Parse input to as.is:
  if(is.logical(as.is)) {
    as.is <- rep_len(as.is, cols)
  } else if(is.numeric(as.is)) {
    if(any(as.is < 1 | as.is > cols))
      stop("invalid numeric 'as.is' expression")
    i <- rep.int(FALSE, cols)
    i[as.is] <- TRUE
    as.is <- i
  } else if(is.character(as.is)) {
    i <- match(as.is, col.names, 0L)
    if(any(i <= 0L))
        warning("not all columns named in 'as.is' exist")
    i <- i[i > 0L]
    as.is <- rep.int(FALSE, cols)
    as.is[i] <- TRUE
  } else if (length(as.is) != cols)
    stop(gettextf("'as.is' has the wrong length %d  != cols = %d",
                  length(as.is), cols), domain = NA)

  what = what[keep]; known = known[keep]
  for (i in (1L:length(x))[!known]) {
    x[[i]] <- if (is.na(colClasses[i]))
        type.convert(x[[i]], as.is = as.is[i], dec = dec, na.strings = na.strings)
    else if (colClasses[i] == "factor")
        as.factor(x[[i]])
    else if (colClasses[i] == "Date")
        as.Date(x[[i]])
    else if (colClasses[i] == "POSIXct")
        as.POSIXct(x[[i]])
    else methods::as(x[[i]], colClasses[i])
  }

  return(x)
}
