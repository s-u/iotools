chunk.reader <- function(source, max.line = 65536L, sep=NULL) .Call(create_chunk_reader, if(inherits(source, "connection")) source else file(as.character(source)[1], "rb"), max.line, sep)

read.chunk <- function(reader, max.size = 33554432L) .Call(chunk_read, reader, max.size)
