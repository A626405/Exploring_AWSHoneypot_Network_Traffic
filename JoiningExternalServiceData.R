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


 working_data |>
  group_by(port) |>
  arrange(.by_group = T)

working_data <- dplyr::left_join(working_data,portmatch_df2, by = c("port"),relationship = "many-to-many",na_matches = "never",,keep = T,multiple = "all",copy = T)

rm(portmatchdata,portmatch1,portmatch_df1)
gc()
rm(portmatch2,portmatch_df2,ncores)
gc()

protfac<-as.numeric(factor(working_data$protocol.x))
indexicmp<-which(protfac==1)
working_data$protocol.y[indexicmp] <- "ICMP"
working_data$service[indexicmp] <- "ICMP"

index0port<-which(working_data$port=="0")
working_data$protocol.y[index0port] <- "Unassigned"
working_data$service[index0port] <- "Unregistered"

index0port<-which(working_data$port=="42776")
working_data$protocol.y[index0port] <- "Unassigned"
working_data$service[index0port] <- "Unassigned"

working_data <- working_data %>% group_by(port) %>% arrange(.by_group=T)


require(tidyr)
port<-c("1433","99999","445","3389","80","56338","8080","22","3306","2193","135","53","23","5060","6666","443","3128","5900","19","1469","21","25","110","143","7","389","123","68","5432","1434","1900","5353","161","179","139","137","111","0","13","9","3","2","1","37","33","26","38","42")
services <- c("MSSQL_Server","ICMP","SMB","RDP","HTTP","UDP_Flood1","HTTPAlt","SSH","MySQL","UDP_Flood2","RPC","DNS","Telnet","SIP","IRC","HTTPS","Squid_Proxy","VNC","CHARGEN","AAL_LM","FTP","SMTP","POP3","IMAP","Echo","LDAP","NTP","DHCPClient","PostgreSQL","MSSQL_Monitor","SSDP","MDNS","SNMP","BGP","NETBIOS_ssh","NETBIOS_ns","RCPBind","Reserved","Daytime","Discard","compressnet","compressnet","tcpnux","time","dsp","unassigned","rap","nameserver_WIN")

servdict<-data.frame(cbind(c(1:48),port,services))
servdict<-data.frame(cbind(port,services))





ddos_grouped <- working_data |>
  left_join(servdict,relationship="many-to-many",na_matches = "never",multiple="all",copy=T)


merge(servdict,)