library(sqldf)
library(dplyr)

#从字符串中提取时间
Date_getMon = function(x){
  s <- substr(x,3,5)
  if(s == "jan")
    return(1)
  if(s == "feb")
    return(2)
  if(s == "mar")
    return(3)
  if(s == "apr")
    return(4)
  if(s == "may")
    return(5)
  if(s == "jun")
    return(6)
  if(s == "jul")
    return(7)
  if(s == "aug")
    return(8)
  if(s == "sep")
    return(9)
  if(s == "oct")
    return(10)
  if(s == "nov")
    return(11)
  if(s == "dec")
    return(12)
  return("null")
}
#从字符串中提取年份
Date_getYear = function(x)
{
  s <- substr(x,6,9)
  s <- as.numeric(s)
  return(s)
}
#切割9位cusip的函数
str_slice <- function(x){
  s = substr(x,1,6)
  return(s)
}

#获取n个月前的收入
winpre <- function(x,w){
  month = as.numeric(x['month'])
  year = as.numeric(x['year'])
  pre_month = (month - w +12) %% 12
  if(pre_month==0)
    pre_month = 12
  if(month<=w)
    pre_year = year - 1 
  else
    pre_year = year
  if(pre_year>=2003)
    x['ret'] = ret[which((ret$year==pre_year)&(ret$month==pre_month)),'ret']
  else
    x['ret'] = NA
  return(x)
}
#获取n个月后的收入
winlater <- function(x,w){
  month = as.numeric(x["month"])
  year = as.numeric(x['year'])
  later_month = (month + w) %% 12
  if(later_month==0)
    later_month=12
  if(month>12-w)
    later_year = year + 1 
  else
    later_year = year
  if(pre_year<=2015)
    x['ret'] = ret[which((ret$year==pre_year)&(ret$month==pre_month)),'ret']
  else
    x['ret'] = NA
  return(x)
}

##########################################################################################
#读取四张原始表
setwd("C:\\data")
revere <- read.csv("revere_06July.csv",stringsAsFactors = FALSE)
mktret <- read.csv("market return.csv",stringsAsFactors = FALSE)
ret <- read.csv("returns.csv",stringsAsFactors = FALSE)
ma_event <- read.csv("MA_SDC.csv",stringsAsFactors = FALSE)
#去掉所有dateeffective为空值的行
ma_event <- ma_event[ma_event$DateEffective != "",]
#提取ma_event表中的字符串的相应年份与月份，并将时间限制在2003至2015年
ma_event$YearAnnounced <- Date_getYear(ma_event$DateAnnounced)
ma_event <- ma_event[(ma_event$YearAnnounced >= 2003) & (ma_event$YearAnnounced <= 2015),]
ma_event$MonAnnounced  <- c(1:nrow(ma_event))
ma_event$MonAnnounced <- lapply(ma_event$DateAnnounced,Date_getMon)
ma_event$YearEffective <- Date_getYear(ma_event$DateEffective)
ma_event$MonAnnounced  <- c(1:nrow(ma_event))
ma_event$MonEffective <- lapply(ma_event$DateEffective,Date_getMon)
ma_event_cut <- ma_event
write.csv(ma_event, file="MA_SDC_CUT.csv", col.name = colnames(ma_event))
nrow_ma <- nrow(ma_event)
ma_event = as.data.frame(ma_event)
ma_event$dataValid  <- c(1:nrow_ma)

#得到供应链9位cusip码中的前6位
revere$c_cusip <- lapply(revere$c_cusip,str_slice)
revere$s_cusip <- lapply(revere$s_cusip,str_slice)
ret$cusip <- lapply(ret$cusip,str_slice)

###########################################################################################
#TARGET EVENT, SUPPLY EFFECT

#将customer码与ma中acquiror合并
ma_event$c_cusip <- ma_event$TargetCUSIP
ma_event$year <- ma_event$YearEffective
ma_event$month <- ma_event$MonEffective
t_supply <- merge(ma_event, revere, c("c_cusip","year","month"), all=FALSE)

#取窗口
ret1 <- ret
ret2 <- ret
mktret1 <- mktret
mktret2 <- mktret
ret1 = apply(ret,1,winpre,w=3)
ret2 = apply(ret,1,winlater,w=3)
mktret1 = apply(mktret,1,winpre,w=3)
mktret2 = apply(mktret,1,winlater,w=3)
ret_simple1 <- ret1[,c(6:9)]
ret_simple2 <- ret2[,c(6:9)]
#将ma_event与market return, return合并起来
names(ret_simple1)[1] = "s_cusip"
names(ret_simple1)[2] = "s_ret"
names(ret_simple2)[1] = "s_cusip"
names(ret_simple2)[2] = "s_ret"
ma_event_temp1 <- merge(t_supply, ret_simple1, c("s_cusip","year","month"), all=FALSE)
ma_event_temp2 <- merge(t_supply, ret_simple2, c("s_cusip","year","month"), all=FALSE)
ma_event_temp1 <- merge(ma_event_temp1, mktret1, c("year","month"),all = FALSE)
ma_event_temp2 <- merge(ma_event_temp2, mktret2, c("year","month"),all = FALSE)
#算RetC
ma_event_temp1$s_ret = ma_event_temp1$s_ret - ma_event_temp1$rm
ma_event_temp1$month <- as.numeric(ma_event_temp1$month)
ma_event_temp2$s_ret = ma_event_temp2$s_ret - ma_event_temp2$rm
ma_event_temp2$month <- as.numeric(ma_event_temp2$month)
ma_event_temp1$s_ret <- ma_event_temp2$s_ret - ma_event_temp1$s_ret
ma_event_temp1 = group_by(ma_event_temp1,c_cusip,year,month)
tsret1 <- summarise(ma_event_temp1,NRet = mean(ma_event_temp1$s_ret,na.rm = TRUE))
r = c(tsret$NRet)
t.test(r,mu=0,alternative = "greater")


###########################################################################################
#TARGET EVENT, CUSTOMER EFFECT

#将supplier码与ma中acquiror合并
ma_event$s_cusip <- ma_event$TargetCUSIP
t_customer <- merge(ma_event, revere, c("s_cusip","year","month"), all=FALSE)

#取窗口
#将ma_event与market return, return合并起来
names(ret_simple1)[1] = "c_cusip"
names(ret_simple1)[2] = "c_ret"
names(ret_simple2)[1] = "c_cusip"
names(ret_simple2)[2] = "c_ret"
ma_event_temp1 <- merge(t_customer, ret_simple1, c("c_cusip","year","month"), all=FALSE)
ma_event_temp2 <- merge(t_customer, ret_simple2, c("c_cusip","year","month"), all=FALSE)
ma_event_temp1 <- merge(ma_event_temp1, mktret1, c("year","month"),all = FALSE)
ma_event_temp2 <- merge(ma_event_temp2, mktret2, c("year","month"),all = FALSE)
#算RetC
ma_event_temp1$c_ret = ma_event_temp1$c_ret - ma_event_temp1$rm
ma_event_temp1$month <- as.numeric(ma_event_temp1$month)
ma_event_temp2$c_ret = ma_event_temp2$c_ret - ma_event_temp2$rm
ma_event_temp2$month <- as.numeric(ma_event_temp2$month)
ma_event_temp1$c_ret <- ma_event_temp2$c_ret - ma_event_temp1$c_ret
ma_event_temp1 = group_by(ma_event_temp1,s_cusip,year,month)
tcret1 <- summarise(ma_event_temp1,NRet = mean(ma_event_temp1$c_ret,na.rm = TRUE))
r = c(tcret$NRet)
t.test(r,mu=0,alternative = "greater")
