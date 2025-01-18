working_data<-readRDS("data/internal/rds/working_data.RDS")
require(vroom)
require(dplyr)
gc()
ncores<-parallel::detectCores()-1
portmatchdata<- vroom::vroom("data/external/service-names-port-numbers.csv",num_threads=15,delim=",",col_select=c("Service Name","Port Number","Transport Protocol"),col_names=T,skip_empty_rows=T,na=c("","NA"),escape_double=T)
gc()
portmatchdata<-na.omit(portmatchdata)
colnames(portmatchdata) <- c("service","port","protocol")

portmatchdata <- as.data.frame(portmatchdata)

portmatch1<-c(match(portmatchdata$port,working_data$port))
portmatch_df1<-na.omit(data.frame("protocol"=working_data$protocol[portmatch1],"port"=working_data$port[portmatch1]))
gc()
portmatch2<-c(match(portmatch_df1$port,portmatchdata$port))
portmatch_df2<-unique(data.frame("protocol"=portmatchdata$protocol[portmatch2],"port"=portmatchdata$port[portmatch2],"service"=portmatchdata$service[portmatch2]))

working_data$port <- as.character(working_data$port)
portmatch_df2$protocol <- toupper(portmatch_df2$protocol)
portmatch_df2$port <- as.character(portmatch_df2$port)
  

working_data1 <- working_data |>
  left_join(portmatch_df2, by = c("port"),relationship = "many-to-many",na_matches = "na",multiple = "all",copy = T) |>
  group_by(datetime) |>
  arrange(.by_group = T)


rm(portmatchdata,portmatch1,portmatch_df1)
gc()
rm(portmatch2,portmatch_df2,ncores)
gc()
