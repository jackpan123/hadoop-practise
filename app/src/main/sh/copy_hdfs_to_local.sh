#!/usr/bin/env bash

for i in {1903..1920};
do
  $HADOOP_HOME/bin/hadoop fs -copyToLocal /user/root/gz/home/jack-pan-loganalyzes/$i.gz /home/jack-pan-loganalyzes/all_year_gz_file/$i.gz
  echo "reporter:status:Processed $i.gz" >&2
done