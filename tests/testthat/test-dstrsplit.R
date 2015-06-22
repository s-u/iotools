library(testthat)
library(iotools)

n = 100
test_df = data.frame(col1 = sample(state.abb,n,TRUE),
                     col2 = sample(1:10,n,TRUE),
                     col3 = runif(n),
                     col4 = complex(n,runif(n),runif(n)),
                     col5 = charToRaw(paste(sample(letters,n,TRUE),collapse="")),
                     col6 = runif(n) > 0.5,
                     stringsAsFactors=FALSE)
colClasses = sapply(test_df, class)

# Standard output
out = as.output(test_df)
expect_equal(class(out), "raw")
expect_equal(dstrsplit(out,col_types=colClasses),test_df)

# With rownames
rownames(test_df) = basename(sapply(1:n,function(v)tempfile()))
out = as.output(test_df)
expect_equal(class(out), "raw")

dout = dstrsplit(out,col_types=colClasses,nsep="\t") # won't be perfect copy
expect_equal(dout$rowindex, rownames(test_df))
rownames(dout) = dout$rowindex
dout = dout[,-1]
expect_equal(dout, test_df)

rownames(test_df) = NULL

# Matrix with one row
out = as.output(test_df[1,])
expect_equal(class(out), "raw")
expect_equal(dstrsplit(out,col_types=colClasses),test_df[1,])

# Matrix with one column
out = as.output(test_df[,1,drop=FALSE])
expect_equal(class(out), "raw")
expect_equal(dstrsplit(out,col_types=colClasses[1]),test_df[,1,drop=FALSE])

# Matrix with zero columns and rows
test_df_empty = data.frame(test_df[NULL,])
out = as.output(test_df_empty)
expect_equal(class(out), "raw")
expect_equal(length(out), 0L)
expect_equal(dstrsplit(out,col_types=colClasses),test_df_empty)

# Data frame with dos line endings:
this_test_df = test_df
this_test_df[,ncol(this_test_df)] = paste0(this_test_df[,ncol(this_test_df)], "\r")
out = as.output(this_test_df)
expect_equal(dstrsplit(out,col_types=colClasses),test_df)

# Data frame with quotes around character column
this_test_df = test_df
this_test_df[,1] = paste0("'",this_test_df[,1], "'")
out = as.output(this_test_df)
expect_equal(dstrsplit(out,col_types=colClasses, quote="'\""),test_df)

# Data frame with bad quotes
this_test_df = test_df
this_test_df[,1] = paste0("'", this_test_df[,1])
out = as.output(this_test_df)
expect_error(dstrsplit(out,col_types=colClasses, quote="'\""),
  "End of line within quoted string.")

