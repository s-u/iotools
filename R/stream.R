chunk.apply <- function(input, FUN, ..., CH.MERGE=rbind, CH.MAX.SIZE=33554432) {
  if (!inherits(input, "ChunkReader"))
    reader <- chunk.reader(input)
  .Call(chunk_apply, reader, CH.MAX.SIZE, CH.MERGE, FUN, parent.frame(), .External(pass, ...))
}

chunk.tapply <- function(input, FUN, ..., sep='\t', CH.MERGE=rbind, CH.MAX.SIZE=33554432) {
  if (!inherits(inherits, "ChunkReader"))
    reader <- chunk.reader(input)
  .Call(chunk_tapply, reader, CH.MAX.SIZE, CH.MERGE, sep, FUN, parent.frame(), .External(pass, ...))
}
