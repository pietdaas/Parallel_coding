##Bootstrap examples serieel and parallel

##load libraries
library(parallel)
library(doParallel)
library(foreach)

##Start with serieel (1 core), data set used is mtcars
mtcars
##Number of bootstraps 
n <- 10000
result <- numeric()

##Example 1, bootstrap #######################################################

##Serieel (with for loop)
##1. Serieel version 1
start <- Sys.time()

for(i in 1:n) {
  ##Select data
  d <- mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]
  fit <- lm(formula=mpg~wt+disp, data=d)
  result[i] <- summary(fit)$r.square
}

end <- Sys.time()
end - start ##takes 8.738536 secs

length(result)
hist(result, breaks=100, col="blue")


##2. Serieel version 2
start <- Sys.time()

for(i in 1:n) {
  ##Select data
  d <- mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]
  result[i] <- summary(lm(formula=mpg~wt+disp, data=d))$r.square
}

end <- Sys.time()
end - start  ##takes 8.173388 secs

length(result)
hist(result, breaks=100, col="blue")

##3. Serieel version 3
start <- Sys.time()

for(i in 1:n) {
  ##Select data
  result[i]<-summary(lm(formula=mpg~wt+disp, data=mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]))$r.square
}

end <- Sys.time()
end - start  ##takes 8.057574 secs

length(result)
hist(result, breaks=100, col="blue")


##4. Serieel with sapply
start <- Sys.time()

result <- sapply(1:n, function(i) {
  summary(lm(formula=mpg~wt+disp, data=mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]))$r.square
})

end <- Sys.time()
end - start  ##takes 8.27093 secs

length(result)
hist(result, breaks=100, col="blue")


##5. Serieel with replicate
bootFun <- function(x) {
  summary(lm(formula=mpg~wt+disp, data=x[sample(nrow(x), nrow(x), replace=T), ]))$r.square
}

start <- Sys.time()

result <- replicate(n, bootFun(mtcars)) 

end <- Sys.time()
end - start  ##takes 7.991064 secs

length(result)
hist(result, breaks=100, col="blue")

##############################################################
##Create cluster (for foreach loop)
registerDoParallel(cores = detectCores())
cores <- detectCores()
print(paste0("number of cores detected: ", cores))

##6. Parallel, foreach loop ran n times
start <- Sys.time()

result <- foreach(i=1:n, .combine='c') %dopar% {
  summary(lm(formula=mpg~wt+disp, data=mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]))$r.square
}

end <- Sys.time()
end - start  ##takes 3.574135 secs

length(result)
hist(result, breaks=100, col="blue")


##7. Parallel, foreach loop, each core n/cores times (replicate)
start <- Sys.time()

result <- foreach(i=1:cores, .combine='c') %dopar% {
    replicate(n/cores, bootFun(mtcars)) 
}

end <- Sys.time()
end - start  ##takes 2.320392 secs

length(result)
hist(result, breaks=100, col="blue")

##8. Parallel with sapply
start <- Sys.time()

result <- foreach(i=1:cores, .combine='c') %dopar% {
  sapply(1:(n/cores), function(i) {
    summary(lm(formula=mpg~wt+disp, data=mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]))$r.square
  })
}

end <- Sys.time()
end - start  ##takes 2.381618 secs

length(result)
hist(result, breaks=100, col="blue")


##9. Parallel with sapply (chunks)
start <- Sys.time()

result <- foreach(b=idiv(n, chunks=getDoParWorkers()), .combine='c') %dopar% {
  sapply(1:b, function(i) {
    summary(lm(formula=mpg~wt+disp, data=mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]))$r.square
  })
}

end <- Sys.time()
end - start  ##takes 2.372167 secs

length(result)
hist(result, breaks=100, col="blue")


##10. Parallel with replicate (bootFun) and chunks
start <- Sys.time()

result <- foreach(b=idiv(n, chunks=getDoParWorkers()), .combine='c') %dopar% {
  replicate(b, bootFun(mtcars)) 
}

end <- Sys.time()
end - start  ##takes 2.353541 secs

length(result)
hist(result, breaks=100, col="blue")

##stop cluster
stopImplicitCluster()

##try parallel apply functions
###Alternative way of creating cluster (required for parallel sapply)
cl <- makeCluster(getOption("cl.cores", detectCores()))

##11. Parallel with parallel sapply (in parallel package) ##NEEDS cluster cl defined
start <- Sys.time()

result <- parSapply(cl, 1:n, FUN=function(i) { 
  summary(lm(formula=mpg~wt+disp, data=mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]))$r.square
})

end <- Sys.time()
end - start  ##takes 2.597357 secs

length(result)
hist(result, breaks=100, col="blue")

##stop cluster
stopCluster(cl)


##Define implicit cluster for next approach
registerDoParallel(cores = detectCores())


##Boostratpping with Boot package, serial and parallel
library(boot)
##function to obtain R-squared from the data
rsq <- function(formula, data, indices) {
  d <- data[indices, ]
  fit <- lm(formula, data=d)
  return(summary(fit)$r.square)
}

##12. boot SINGLE core
start <- Sys.time()

results <- boot(data=mtcars, statistic=rsq, R=n, formula=mpg~wt+disp)

end <- Sys.time()
end - start  ##takes 8.971382 secs

length(results$t)
plot(results)


##13. boot multicore (implicit cluster), DOES NOT WORK UNDER WINDOWS!!
start <- Sys.time()

results <- boot(data=mtcars, statistic=rsq, R=n, formula=mpg~wt+disp, parallel="multicore", ncpus=cores)

end <- Sys.time()
end - start  ##takes 2.447988 secs

length(results$t)
plot(results)


##14. boot in parallel foreach loop
start <- Sys.time()

results2 <- foreach(i=1:cores, .combine='c', .packages='boot') %dopar% {
  boot(data=mtcars, statistic=rsq, R=n/cores, formula=mpg~wt+disp)
}

end <- Sys.time()
end - start  ##takes 2.338203 secs

length(results2$t)
plot(results2)


##15. boot in parallel loop with multicore
start <- Sys.time()

results2 <- foreach(i=1:cores, .combine='c', .packages='boot') %dopar% {
  boot(data=mtcars, statistic=rsq, R=n/cores, formula=mpg~wt+disp, parallel="multicore", ncpus=cores)
}

end <- Sys.time()
end - start ##takes 3.669705 secs

length(results2$t)
plot(results2)

##stop cluster
stopImplicitCluster()


##Effect of using different number of cores of a predefinned cluster#############################################################################
detectCores()
##start implicit cluster
registerDoParallel(cores = detectCores())

##16. Effect of various numbers of virtual cores/threads (1-20) on cluster (size of cluster is unchanged)
res <- as.double()
for(k in 1:20) {
    
  start <- Sys.time()
  result <- foreach(i=1:k, .combine='c') %dopar% {
              sapply(1:ceiling(n/k), function(i) {
              summary(lm(formula=mpg~wt+disp, data=mtcars[sample(nrow(mtcars), nrow(mtcars), replace=T), ]))$r.square
            })
  }
  end <- Sys.time()  
  res[k] <- as.numeric(end-start)
  print(paste(k, length(result), as.character(res[k]), sep=" "))
  gc()
}
##Plot results
plot(c(1:20), res, type="b", xlab="Number of parallel processes", ylab="Total time (sec)", main = paste0("Number of cores: ", cores))
 
##stop cluster
stopImplicitCluster()























