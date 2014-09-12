### High-performance I/O tools for R

Anyone dealing with large data knows that stock tools in R are bad at
loading (non-binary) data to R. This package started as an attempt to
provide high-performance parsing tools that minimize copying and avoid
the use of strings when possible (see
[mstrsplit](http://rforge.net/doc/packages/iotools/mstrsplit.html),
for example).

To allow processing of arbitrarily large files we have added way to
process chunk-wise input, making it possible to compute on streaming
input as well as very large files (see
[chunk.reader](http://rforge.net/doc/packages/iotools/chunk.html) and
[chunk.apply](http://rforge.net/doc/packages/iotools/chunk.apply.html)).

The next natural progress was to wrap support for Hadoop
streaming. The major goal was to make it possible to compute using
Hadoop Map Reduce by writing code that is very natural - very much
like using `lapply` on data chunks without the need to know anything
about Hadoop. See [the WiKi page](https://github.com/s-u/iotools/wiki)
for the idea and
(hmr)[http://rforge.net/doc/packages/iotools/hadoop.html] function for
the documentation.
