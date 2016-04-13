
# BigData Log High Availability Import Framework

Welcome to STREAMS!

## Introduction

Streams is a high availability, extremely fast, low resource usage real time log collection framework for terrabytes of data.

## Downloads

Version 0.2.0 and below are available from the Downloads link, for more up to date versions please use sourceforge:

http://sourceforge.net/projects/bigstreams/files/

## Links

  * [Kafka Log Collection](https://github.com/gerritjvv/bigstreams/wiki/KafkaToHadoopImport.wiki)
  * [Setup and Installation](https://github.com/gerritjvv/bigstreams/wiki/StreamsSetupAndInstallation.wiki)
    * [Zookeeper](https://github.com/gerritjvv/bigstreams/wiki/zookeeperInstallationGuide.wiki)
    * [Collector Setup](https://github.com/gerritjvv/bigstreams/wiki/CollectorSetupAndInstallationGuide.wiki)
    * [Agent Setup](https://github.com/gerritjvv/bigstreams/wiki/AgentSetupAndinstallationGuide.wiki)
    * [LZO Compression](https://github.com/gerritjvv/bigstreams/wiki/StreamsAndLzoCompression.wiki)
  * User Guides
    * [Collector User Guide](https://github.com/gerritjvv/bigstreams/wiki/CollectorUserGuide.wiki)
    * [Agent User Guide](https://github.com/gerritjvv/bigstreams/wiki/AgentUserGuide.wiki)
    * [Agent File Log Management](https://github.com/gerritjvv/bigstreams/wiki/AgentFileLogActionManagement.wiki)
  * [Streams and Data Integrity](https://github.com/gerritjvv/bigstreams/wiki/StreamsAndDataIntegrity.wiki)
  * [Production Operational Checks](https://github.com/gerritjvv/bigstreams/wiki/NagiosProductionOperationalChecks.wiki)
  * [Java](https://github.com/gerritjvv/bigstreams/wiki/RecommendedJVMVersion.wiki)
  * [File Errors Recovery](https://github.com/gerritjvv/bigstreams/wiki/RecoverLzoCorruptFiles.wiki)

## Project Aims

Streams main aims to

High Availability for big data log import  
Maintain data correctness  
Be scalable to terrabytes of data per day.  
Provide integration with hadoop for importing data into hadoop hdfs.  

## Overview

Streams is inspired by Chukwa, an apache hadoop project for importing hadoop log data for monitoring of clusters. Streams aims to provided support for collecting application log data, i.e. not debug information but application logs such as Adserver Logs, Transactional Logs for banking etc.  
 
These logs cannot afford any data loss, data corruption or row duplication. Files are normally in the terrabytes spread accross a cluster of servers. Streams is used to import these data to a smaller cluster 2,3 machines of collectors, then import the collector compressed data into HDFS.  

Logs collected are partitioned per date,hour and size, allowing administrators to specify the chunk sizes of collected logs. e.g. Lets say we have log type A and we want to use this on a hadoop cluster for block size 128MB. Streams can import all logs for type A base on daydate and hour and in chunks more or less in 128MB size. This makes the files easier to process in M/R and allows non splittable compression formats to be used.  

## Contact

Email: gerritjvv@gmail.com

Twitter: @gerrit_jvv

## License


Distributed under the Eclipse Public License either version 1.0 
