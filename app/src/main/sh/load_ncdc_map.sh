#!/usr/bin/env bash
#  Datasets are available for download on this FTP link. Data is partitioned by year starting from 1901 to till date.
#ftp://ftp.ncdc.noaa.gov/pub/data/noaa/
# Copy these year dataset and upload to my linux machine, execute this shell to load data to HDFS.

# Un-gzip each station file and concat into one file
echo "reporter:status:Un-gzipping $1" >&2
for i in {1901..1920};
do
  echo $i;
  target=$1$i
  for file in $target/*
  do
    gunzip -c $file >> $target.all
    echo "reporter:status:Processed $file" >&2
  done

  # Put gzipped version into HDFS
  echo "reporter:status:Gzipping $target and putting in HDFS" >&2
  gzip -c $target.all | $HADOOP_HOME/bin/hadoop fs -put - gz/$target.gz
done

