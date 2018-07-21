#Map-Reduce Analysis#

Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/hdp/2.3.0.0-2557/hadoop-mapreduce/hadoop-streaming-2.7.1.2.3.0.0-2557.jar")

detach("package:rhdfs", unload=TRUE)
detach("package:rmr2", unload=TRUE)
library("rhdfs", lib.loc="/usr/lib64/Revo-7.4/R-3.1.3/lib64/R/library")

hdfs.init()

# Specify the path
hdfs.root = '/user/share/student'
# append the data filename to the pathname 
flight.hdfs.data = file.path(hdfs.root,'test_203694.csv')
# append the output filename to the pathname

carrier.hdfs.out=file.path(hdfs.root, "Carrier")
origin.hdfs.out=file.path(hdfs.root, "Origin")

mapflights = function(k,flights) 
{
  return (keyval(as.character(flights[[17]]),as.integer(flights[[16]])))
}
reduceorigin = function(origin, counts) 
{
  average = sum(counts,na.rm=TRUE)/length(counts)
  keyval(origin, average)
}

OriginMr1 = function(input, output = NULL) {
  mapreduce(input = input,
            output = output,
            input.format = make.input.format("csv", sep=","),
            map = mapflights,
            reduce = reduceorigin)}

out = OriginMr1(flight.hdfs.data, origin.hdfs.out)

results = from.dfs(out)
results.df = as.data.frame(results, stringsAsFactors=F)
colnames(results.df) = c('Origin', 'Departure Delay')
results.df 


mapcarrier = function(k,flights) 
{
  return (keyval(as.character(flights[[9]]),as.integer(flights[[16]])))
}
reducecarrier = function(carrier, counts) 
{
  average = sum(counts,na.rm=TRUE)/length(counts)
  keyval(carrier, average)
}

CarrierMr1 = function(input, output = NULL) {
  mapreduce(input = input,
            output = output,
            input.format = make.input.format("csv", sep=","),
            map = mapcarrier,
            reduce = reducecarrier)}

out2 = CarrierMr1(flight.hdfs.data, carrier.hdfs.out)

results = from.dfs(out2)
results.df = as.data.frame(results, stringsAsFactors=F)
colnames(results.df) = c('Carrier', 'Departure Delay')
results.df 


sum(originresults.df[,2])/280
sum(carrierresults.df[,2])/19
var(originresults.df[,2])
var(carrierresults.df[,2])






