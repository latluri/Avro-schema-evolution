#!/bin/bash

# run this script as shown below
# sh *.sh

#This script is written by Laxmi Narayana Atluri, CSID latluri1


#set PYSPARK_DRIVER_PYTHON

kinit  $USER@HPC.FORD.COM -k -t latluri1_keytab


#kinit $USER@HPC.FORD.COM -k -t /s/$USER/.$USER.krb5.keytab


unset PYSPARK_DRIVER_PYTHON


export SPARK_MAJOR_VERSION=2 
export SPARK_HOME=/usr/hdp/current/spark2-client 
export HADOOP_CLIENT_OPTS="-Xmx5g" 

#output_tz=`spark-submit   --driver-memory 2g   --jars spark-avro_2.11-4.0.0.jar json2hive.py `


output_tz=`spark-submit --driver-memory 16g --executor-memory 12g  --master yarn --conf spark.driver.maxResultSize=5G  --jars spark-avro_2.11-4.0.0.jar json2hive.py `


#output_tz=`spark-submit --version`

echo -e " $output_tz" | mailx -s 'J2H FSM data load status'   latluri1@ford.com
