##Processing large data file examples, serieel and parallel

##load libraries
library(parallel)
library(doParallel)
library(foreach)
library(stringdist)

##get data, file is a road sensor data file with more then 2 million rows
data <- read.csv(file = "Large_file_traffic.csv", header = TRUE, sep = ",", dec = ".", quote = "", stringsAsFactors = FALSE)
##data <- read.csv(file = unz("Large_file_traffic.zip", "Large_file_traffic.csv"), header = TRUE, sep = ",", dec = ".", quote = "", stringsAsFactors = FALSE)
nrow(data)

##add countA
data$countA <- -1
measurementRefs <- data$measurementSiteReference


##define function countA; count number of letter 'a' in string
countA <- function(string) {
  cntA <- 0
  ##check input
  string <- tolower(as.character(string))
  if(nchar(string) > 0) {
    res <- unlist(gregexpr(pattern = "a", text = string))
    if(res[1] > 0) {
      cntA <- length(res)
    } else {
      cntA <- 0
    }
  }
  return(cntA)
}


##Start with serieel (1 core)
##Example 1, count a in name provided in loop
data$countA <- -1

start <- Sys.time()

for(i in 1:nrow(data)) {
 data$countA[i] <- countA(data$measurementSiteReference[i]) 
}

end <- Sys.time()
end - start ##take extremely long (1.392687 hours, 1.421784 hours) show how slow for loops can be in R
sum(data$countA) ##2557345

##Example 2, count a with apply version
data$countA <- -1
start <- Sys.time()

data$countA <- sapply(X = data$measurementSiteReference, FUN = function(x) countA(x))

end <- Sys.time()
end - start ##takes 24.01224 seconds, no need to speed this up
sum(data$countA) ##2557345

#####################################################################################################################################
##USE ANOTHER EXAMPLE
##Example 3, determine stringsimilarity for names, use name of first sensor to compare the rest
data$countA <- -1

start <- Sys.time()

data$countA <- sapply(X = data$measurementSiteReference, FUN = function(x) stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x))

end <- Sys.time()
end - start ##takes 4.105475 mins
sum(data$countA) ##1035019


##What is the effect of a parallelisation?

##Example 4 use mclapply (a parallel version of lapply)
cores <- detectCores()
data$countA <- -1

start <- Sys.time()

data$countA <- unlist(mclapply(X = data$measurementSiteReference, FUN = function(x) stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x), mc.cores = cores))

end <- Sys.time()
end - start  ##takes 59.18108 secs
sum(data$countA) ##1035019

##Examole 5 use mclapply but with only the vector (not the frame)
measurementRefs <- data$measurementSiteReference
data$countA <- -1

start <- Sys.time()

data$countA <- unlist(mclapply(X = measurementRefs, FUN = function(x) stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x), mc.cores = cores))

end <- Sys.time()
end - start  ##takes 52.15732 secs
sum(data$countA) ##1035019

##Example 6 use parSapply with a defined cluster
cl <- makeCluster(getOption("cl.cores", detectCores()))
data$countA <- -1

start <- Sys.time()

data$countA <- parSapply(cl = cl, X = measurementRefs, FUN = function(x) stringdist:::stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x))

end <- Sys.time()
end - start ##takes 1.123854 mins
sum(data$countA) ##1035019

##Example 7 parSapply with load balancing (to check if that speeds things up)
data$countA <- -1

start <- Sys.time()

data$countA <- parSapplyLB(cl = cl, X = measurementRefs, FUN = function(x) stringdist:::stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x))

end <- Sys.time()
end - start  ##takes 1.072466 mins
sum(data$countA) ##1035019

##stop cluster
stopCluster(cl)

##Create cluster (for foreach loop)
registerDoParallel(cores = detectCores())
cores <- detectCores()
print(paste0("number of cores detected: ", cores))

##Example 8a, divide blocks of data over cluster with iterator
data$countA <- -1
n <- as.numeric(nrow(data))

start <- Sys.time()

data$countA <- foreach(i=split(1:n, 1:cores), .combine='c', .packages = "stringdist") %dopar% {
  sapply(X = measurementRefs[i], FUN = function(x) stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x))
}

end <- Sys.time()
end - start ##takes 53.2065 secs, Warning is result of fact that 2068998 can not be exactly equally divided over number of cores (8)
sum(data$countA) ##1035019

##Example 8b, divide blocks of data over cluster with iterator, chunks containing subsequent series of numbers
data$countA <- -1
n <- as.numeric(nrow(data))
##define chunk function, to create 8 subsequent series of numbers
chunk <- function(x,n) split(x, factor(sort(rank(x)%%n)+1))

start <- Sys.time()

data$countA <- foreach(i=chunk(1:n, cores), .combine='c', .packages = "stringdist") %dopar% {
  sapply(X = measurementRefs[i], FUN = function(x) stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x))
}

end <- Sys.time()
end - start ##takes 52.07237 secs
sum(data$countA) ##1035019

##Example 9, foreach loop with a list of measurmentRefs equal to the number of cores
measurementList <- split(measurementRefs, 1:cores)
str(measurementList)
length(measurementList)

data$countA <- -1

start <- Sys.time()

data$countA <- foreach(i=1:cores, .combine='c', .packages = "stringdist") %dopar% {
  sapply(X = measurementList[i], FUN = function(x) stringsim(a = "RWS01_MONIBAS_0010vwm0289ra", b = x))
}

end <- Sys.time()
end - start ##takes 1.273706 secs!!!!!
sum(data$countA) ##1035019

##stop cluster
stopImplicitCluster()

