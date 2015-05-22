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
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="character"),test_matrix)

# Complex matrix
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="character"),test_matrix)

# Logical matrix
test_matrix = matrix(sample(state.abb, p*n, TRUE),ncol=p)
out = as.output(test_matrix)
expect_equal(class(out), "raw")
expect_equal(mstrsplit(out,type="character"),test_matrix)


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

