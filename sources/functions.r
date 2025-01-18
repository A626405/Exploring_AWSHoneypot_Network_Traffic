#Main Functions

rsf<-function(){
  gc()
  rm(list=ls(all.names = T,envir = as.environment(".GlobalEnv")),envir = as.environment(".GlobalEnv"),inherits = T)
  gc()
}
#-----------------------------------------------------------
rss <- function() {
  cat("\014")
  gc()  
  all_objs <- ls(all.names = TRUE, envir = .GlobalEnv)
  objs_to_remove <- setdiff(all_objs, "rs")
  rm(list = objs_to_remove, envir = .GlobalEnv)
  cat("\014")
  gc()
}
#-----------------------------------------------------------
rs <- function() {
  cat("\014")
  gc()
  all_objs <- ls(all.names = TRUE, envir = .GlobalEnv)
  obs_keep<-c("rs","maindf")
  objs_to_remove <- setdiff(all_objs,obs_keep)
  rm(list = objs_to_remove, envir = .GlobalEnv)
  cat("\014")
  gc()
}
#-----------------------------------------------------------
'rss <- function(.GlobalEnv) {
  rss <- function() {
    cat("\014")
    gc()
    all_objs <- ls(all.names = TRUE, envir = .GlobalEnv)
    obs_keep<-c("rs","cleaned")
    objs_to_remove <- setdiff(all_objs,obs_keep)
    rm(list = objs_to_remove, envir = .GlobalEnv)
    cat("\014")
    gc()
  }
}'
#-----------------------------------------------------------------------------------------------------
#Function #1: Checks all Cols of A DF for: Class, Count NA/NULL/NaN, Is Categorical?, Count Unique Values
gc(verbose=F,full=F,reset = T)
options(verbose = F,catch.script.errors = T,echo=F)

dfcheck <- function(dataframe, threshold) {
  require(parallel)
  require(foreach)
  require(doParallel)
  require(dplyr)
  
  dataframe <- as.data.frame(dataframe)
  colclasses <- sapply(dataframe, class)
  colname <- c(names(dataframe))
  
  if (!is.integer(threshold)) {
    threshold <- as.integer(threshold)}
  
  num_cores <- detectCores() - 1
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  results <- foreach(colpos = 1:ncol(dataframe), .combine = rbind, .packages = c("foreach", "parallel", "doParallel","dplyr")) %dopar% {
    col_class <- colclasses[colpos]
    num_na <- sum(is.na(dataframe[[colpos]]))
    num_null <- sum(is.null(dataframe[[colpos]]))
    num_nan <- sum(is.nan(dataframe[[colpos]]))
    num_unique_vals <- length(unique(dataframe[[colpos]]))
    is_categorical <- ifelse(num_unique_vals < threshold, "Yes", "No")
    
    c(col_class,num_na,num_null,num_nan,is_categorical,num_unique_vals)
  }
  results_df <- as.data.frame(results,row.names = c(names(dataframe)))
  colnames(results_df) <- c("Class","#NA","#NULL","#NaN","Categorical","Num_Unique_Vals")
  results_df <- results_df[order(results_df$Class),]
  
  print(results_df)
  stopCluster(cl)
}
#-----------------------------------------------------------------------------------------------------
#Function #2: Loads Libraries W/ Multithreaded Processing
load_lb<-function(){
  num_cores<-parallel::detectCores()-1
  cl<-Parallel::makeCluster(num_cores)
  doParallel::registerDoParallel(cl)
  Libs<-c("dplyr","readr","tidyr","stringi","tibble","tidyr")
  parallel::mclapply(Libs, function(pkg){
    
    if (!requireNamespace(pkg,quietly=T,)) {
      install.packages(pkg,dependencies=T,repo='http://cran.rstudio.com',quiet = T)}
  },mc.cores = num_cores)  
  
  parallel::stopCluster(cl)
  cat("All specified libraries are loaded into the global environment.\n")
}
#-----------------------------------------------------------------------------------------------------
