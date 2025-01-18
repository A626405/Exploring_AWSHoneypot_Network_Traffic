require(dplyr)
require(tidyr)
require(tibble)
require(tidyselect)
#library(lubricate)
#library(stringi)
#library(vroom)




Ports<-c("1433","99999","445","3389","80","56338","8080","22","3306","2193","135","53","23","5060","6666","443","3128","5900","19","1469","21","25","110","143","7","389","123","68","5432","1434","1900","5353","161","179","139","137","111","0","13","9","3","2","1","37","33","26","38","42")
Services <- c("MSSQL_Server","ICMP","SMB","RDP","HTTP","UDP_Flood1","HTTPAlt","SSH","MySQL","UDP_Flood2","RPC","DNS","Telnet","SIP","IRC","HTTPS","Squid_Proxy","VNC","CHARGEN","AAL_LM","FTP","SMTP","POP3","IMAP","Echo","LDAP","NTP","DHCPClient","PostgreSQL","MSSQL_Monitor","SSDP","MDNS","SNMP","BGP","NETBIOS_ssh","NETBIOS_ns","RCPBind","Reserved","Daytime","Discard","compressnet","compressnet","tcpnux","time","dsp","unassigned","rap","nameserver_WIN")

servdict<-data.frame(cbind(c(1:48),Ports,Services))
servdict$portsnum<-as.numeric(Ports)

matchindex<- match(ddos_grouped$port,servdict$portsnum,nomatch = NA)
ddos_grouped$servindex <- matchindex
ddos_grouped<-merge(ddos_grouped,servdict,no.dups = F,incomparables = "NA",by.x = "servindex",by.y = "V1",all = T)
ddos_grouped <- ddos_grouped |> mutate("ports"=NULL,"portsnum"=NULL,"servindex"=NULL) 
rm(Ports,Services,servdict)
gc()

servdict<-data.frame("Protocol"=rbind("TCP","UDP","ICMP"),"matchn"=cbind(as.integer(c(1,2,3))))
matchindex<- match(ddos_grouped$protocol,servdict$Protocol,nomatch = NA)
ddos_grouped$servindex <- matchindex
ddos_grouped<-merge(ddos_grouped,servdict,no.dups=F,incomparables="NA",by.x="servindex",by.y="matchn",all=T)
gc()

ddos_grouped <- ddos_grouped |> 
  mutate("servindex"=NULL,"Ports"=NULL,"Protocol"=NULL) |>
  rename("SRCIP"="srcstr","Region"="region","Host"="host","Month"="month","Port"="port",
         "Day"="day","Time"="time","Datetime"="datetime","Connections"="connections","Protocol"="protocol")

rm(matchindex,servdict)
gc()

#na_indices<-which(is.na(ddos_grouped[,12]))
#ddos_grouped$Protocol[-na_indices]
#ddos_grouped$Services[na_indices]<-replace_na(ddos_grouped$Services[na_indices],"Unknown_Unassigned")
rm(na_indices)
gc()

save(ddos_grouped,file ="data/internal/rda/ddos_grouped.RDA",compress=T)
saveRDS(ddos_grouped,"data/internal/rds/ddos_grouped.RDS",refhook=NULL,compress="gzip")