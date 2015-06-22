library(testthat)
library(iotools)

p = 10
n = 100

# Integer matrix
test_matrix = matrix(sample(1:10, p*n, TRUE),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="integer"),test_matrix)

# Numeric matrix
test_matrix = matrix(runif(p*n),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="numeric"),test_matrix)

# Character matrix
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="character"),test_matrix)

# Raw matrix
test_matrix = matrix(charToRaw(paste(sample(letters,n*p,TRUE),collapse="")),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="raw"),test_matrix)

# Complex matrix
test_matrix = matrix(complex(n*p,runif(n*p),runif(n*p)),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="complex"),test_matrix)

# Logical matrix
test_matrix = matrix(runif(n*p) > 0.5,ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="logical"),test_matrix)


# Matrix with comma seperator
test_matrix = matrix(runif(p*n),ncol=p)
out = as.output(test_matrix, sep=",")
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="numeric",sep=","),test_matrix)

# Matrix with row names
test_matrix = matrix(runif(p*n),ncol=p)
rownames(test_matrix) = sample(letters,n,TRUE)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="numeric",nsep="\t"),test_matrix)

# Matrix with one row
test_matrix = matrix(runif(p*1),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="numeric"),test_matrix)

# Matrix with one column
test_matrix = matrix(runif(1*n),ncol=1)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="numeric"),test_matrix)

# Matrix with zero columns and rows
test_matrix = matrix(0L,ncol=0,nrow=0)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(length(out), 0L)
expect_equal(mstrsplit(out,type="integer"),test_matrix)

# Matrix with dos line endings:
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
test_matrix2 = test_matrix
test_matrix2[,ncol(test_matrix2)] = paste0(test_matrix2[,ncol(test_matrix2)], "\r")
out = as.output(test_matrix2)
expect_equal(mstrsplit(out,type="character"),test_matrix)

# Character matrix with quotes
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
test_matrix2 = test_matrix
test_matrix2[,1] = paste0("'",test_matrix2[,1], "'")
out = as.output(test_matrix2)
expect_equal(mstrsplit(out,type="character", quote="'\""),test_matrix)

# Character matrix bad quotes on first line
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
test_matrix2 = test_matrix
test_matrix2[,1] = paste0("'",test_matrix2[,1])
out = as.output(test_matrix2)
expect_error(mstrsplit(out,type="character", quote="'\""),
  "End of line within quote string on line 1; cannot determine num columns!")

# Character matrix bad quotes on non-first line
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
test_matrix2 = test_matrix
test_matrix2[-1,1] = paste0("'",test_matrix2[-1,1])
out = as.output(test_matrix2)
expect_warning(mstrsplit(out,type="character", quote="'\""),
  "End of line within quoted string!")


