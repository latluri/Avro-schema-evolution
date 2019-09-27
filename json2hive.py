import os
import sys
import re
#import pandas as pd
import commands
import uuid
import json
import ast
import itertools
import pyspark.sql.functions
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import Row
#from pyspark.sql.functions import row_number,lit,monotonically_increasing_id, col, size, length, when, struct, sha2, concat_ws
from collections import Counter
from datetime import datetime,timedelta
from pyspark.sql.window import Window
#from pyspark.sql.functions import to_json, struct
import re
from pyspark.sql.types import StructType, StructField, StringType
#import numpy as np
#from pyspark.sql import SparkSession


from pyspark import SparkContext, SparkConf, HiveContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sqlContext = HiveContext(sparkContext=sc)
sqlCtx = HiveContext(sparkContext=sc)

#########################################################################################################################################################################################
#Pre-requisites
#Audit table in another database
#create table latluri1.fsm_audit ( id string,date_ string,process__id string,gdia_load_date string)
#create the database j2h_fsm 
#create the schema folder inside the database location
#create the raw folder to store the data
#index column used is called "columnindex"
#########################################################################################################################################################################################


today=datetime.today().strftime('%Y-%m-%d')
today_full=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
yesterday = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
day_b4_yesterday = (datetime.today() - timedelta(2)).strftime('%Y-%m-%d')
process__id=str(uuid.uuid1())

db="j2h_fsm1"
db_location=""
#Json_location=""
Json_location=""

sqlContext.sql("use "+db)

#sqlContext.sql("show tables").show()

messages_2_days=commands.getoutput("hadoop fs -ls  "+Json_location+" | awk -F' +' -v g=\"{\" -v h=\"}\" -v q=\"'\" -v s=\":\" '{print g q$8q s q$6q h}'").splitlines()[1:]
msg_2_days_dict={}
for i in messages_2_days:
    dict_=ast.literal_eval(i)
    if dict_.keys()[0]!='':
        msg_2_days_dict.update(dict_)
msg_2_days_dict


list_ids_in_db=sqlContext.table("latluri1.fsm_audit").select("id").rdd.map(lambda l356: l356["id"]).collect()
#list_ids_in_db=[i424["id"] for i424 in list_ids_in_db_row if i424["id"] !='']
#list_ids_in_db

list_2_load_keys=list(set(msg_2_days_dict.keys()).difference(set(list_ids_in_db)))[0:3]
if len(list_2_load_keys)==0:
    print("No Process ids to load")
    exit()
list_2_load_str="/* ".join(filter(None, list_2_load_keys))+"/*"
list_2_load_keys_ids=[str(i429).split("/")[-1] for i429 in list_2_load_keys]

print("\n\nThe following processids are loaded "+str(list_2_load_keys_ids).replace("[","").replace("]",""))


ids_2_load=[[i425,msg_2_days_dict[i425]] for i425 in list_2_load_keys]

#ids_2_load=[i400.split(" ") for i400 in list_ids_2_load]
R = Row('id', 'date_')
ids_2_load_df=sqlContext.createDataFrame([R(i421[0],i421[1]) for i421 in ids_2_load])
ids_2_load_df=ids_2_load_df.withColumn("Process__id", lit(process__id))
ids_2_load_df=ids_2_load_df.withColumn("gdia_load_date", lit(str(today_full)))


#commands.getoutput("hadoop fs -cat "+list_2_load_str+"| perl -p -e 's/{\"entitydata\":/\n{\"entitydata\":/g'| grep -v \"^$\" |hadoop fs -put -f - JSON_INPUT_FSM")
commands.getoutput("hadoop fs -cat "+list_2_load_str+"| perl -p -e 's/{\"entitydata\":/\n{\"entitydata\":/g'| grep -v \"^$\" |hadoop fs -put -f - "+db_location+db+"/raw/JSON_INPUT")

#https://stackoverflow.com/questions/5508509/how-do-i-check-if-a-string-is-valid-json-in-python
def is_json(myjson,index_value,index_col):
    try:
        json_object = json.loads(myjson)
        json_object[index_col]=index_value
    except ValueError, e:
        json_object={"_corrupt_record_data":"True"}
        json_object[index_col]=index_value
    return(json_object)


def create_table_stmnt(db_location,db,table):
    return("""create external table """+table+"""
stored as avro
location '"""+db_location+db+"""/"""+table+"""'
tblproperties('avro.schema.url'='"""+db_location+db+"""/schema/"""+table+"""/"""+table+""".avsc')
""")


def filter_df(from_df,table,deciding_col,condition,**kwargs):
    deciding_col_rename=kwargs.get('deciding_col_rename', None)
    reason=kwargs.get('reason', None)
    list_col_dump_df=kwargs.get('list_col_dump_df', None)
    drop_dec_col_source=kwargs.get('drop_dec_col_source', False)
    if type(list_col_dump_df) is not list: list_col_dump_df = [ list_col_dump_df ]
    if deciding_col in from_df.columns:
	if condition.lower()=="null":
		dump_temp=from_df.filter(col(deciding_col).isNull())
		from_df=from_df.filter(col(deciding_col).isNotNull())
	if condition.lower().replace(" ","")=="notnull":
		dump_temp=from_df.filter(col(deciding_col).isNotNull())
		from_df=from_df.filter(col(deciding_col).isNull())
	else:
		dump_temp=from_df.filter(col(deciding_col)==condition)#dont prefer this, issues with the pyspark filter using sql like statements
		from_df=from_df.filter(col(deciding_col)!=condition)
        dump_temp_count=dump_temp.count()
        if dump_temp_count>0:
	    #from_df=from_df.subtract(dump_temp)
	    #if dump_temp_count!=final.count():
            #	from_df=from_df.subtract(dump_temp)
	    #else:
	    #	print("All issues in data")
            print("INFO: "+reason+": "+str(dump_temp_count))
            if deciding_col_rename: dump_temp=dump_temp.withColumnRenamed(deciding_col,deciding_col_rename)
            if reason: dump_temp=dump_temp.withColumn("reason", lit(reason))
            dump_temp=dump_temp.select(list_col_dump_df)
            if table in tables_in_db:
                print("\nLOADED :"+table+":"+str(dump_temp_count))
                dump_temp=add_columns(dump_temp,table)
                create_update_schema(dump_temp,db_location,db,table=table)
		dump_temp.write.mode("append").format("com.databricks.spark.avro").insertInto(db+"."+table)
            else:
                print("\nLOADED :"+table+":"+str(dump_temp_count))
                hive_create_new_table(dump_temp,db_location,db,table)

            #dump_df=dump_df.unionByName(dump_temp)
        else:
            #print("No issues found")
	    pass
    else:
        #print("No deciding column values found")
	pass
    if drop_dec_col_source: from_df=from_df.drop(deciding_col)
    return(from_df)

def hive_create_new_table(final,db_location,db,table):
        print("NEW_TABLE_ALERT: "+table)
        create_update_schema(final,db_location,db,table)
        #final.write.option("forceSchema", updated_schema).option("path",db_location+db+"/"+table).format("com.databricks.spark.avro").saveAsTable(db+"."+table)
        #final.write.option("path",db_location+db+"/"+table).format("com.databricks.spark.avro").saveAsTable(db+"."+table)
        #sqlContext.sql("drop table if exists "+db+"."+table)
        stmnt=create_table_stmnt(db_location,db,table)
        #print(stmnt)
        sqlContext.sql(stmnt)
        final.write.mode("append").format("com.databricks.spark.avro").insertInto(db+"."+table)



def create_update_schema(final,db_location,db,table):
    base="""{\n  "type" : "record",\n  "name" : "topLevelRecord",\n  "fields" : [ {"""
    schema_end_part="\n  } ]\n}"
    schema_attachment="".join("\n  }, {\n    \"name\" : \""+j16+"\",\n    \"type\" : [ \"string\", \"null\" ],\n    \"default\" :\"\"" for j16 in final.columns)
    updated_schema_1=base+schema_attachment+schema_end_part
    updated_schema=re.sub(r"^{\n  \"type\" : \"record\",\n  \"name\" : \"topLevelRecord\",\n  \"fields\" : \[ {\n  }, {\n","{\n  \"type\" : \"record\",\n  \"name\" : \"topLevelRecord\",\n  \"fields\" : [ {\n",updated_schema_1)
    schema_rdd=sc.parallelize([updated_schema]).coalesce(1,True)
    schema_rdd_df=sqlContext.createDataFrame(schema_rdd, StringType())
    schema_rdd_df.write.mode("overwrite").text(db_location+db+"/"+"schema/"+table,compression=None)
    commands.getoutput(" hadoop fs -mv  "+db_location+db+"/schema/"+table+"/part-00000* "+db_location+db+"/schema/"+table+"/"+table+".avsc")


def  add_columns(final,table):
    col_in_new_data=[j28.lower() for j28 in final.columns]
    col_in_table=sqlContext.table(table).columns
    list_2_add=set(col_in_new_data).difference(set(col_in_table))
    list_2_add_maintain_schema=set(col_in_table).difference(set(col_in_new_data))
    if len(list_2_add_maintain_schema)>0:
        print("INFO: "+table+" JSON contains less columns than expected such as: "+str(list_2_add_maintain_schema).replace("set([","").replace("])",""))
        for i386 in list_2_add_maintain_schema:
            final=final.withColumn(i386,lit(unicode("")))
    if len(list_2_add)>0:
        print("INFO: "+table+" New column(s) added :"+str(list_2_add).replace("set([","").replace("])",""))
        updated_col_table=col_in_table+list(list_2_add)
        final=final.select(*updated_col_table)
    else:
        print("INFO: "+table+" No new columns found appending new messages")
        final=final.select(*col_in_table)
    return(final)



def extract_nested_json(source_dataframe,table,col_table_name,col_nested_data,cols_to_add_hash,index_col):
    df2=source_dataframe.filter(col(col_table_name).eqNullSafe(table))
    df2_1_col=df2.select(col_nested_data,index_col).rdd.map(lambda p1:is_json(p1[col_nested_data],p1[index_col],index_col)).map(lambda g2: [k.lower() for k in g2.keys()]).reduce(lambda h77,h78: list(set(h77+h78)))
    df2_1=df2.select(col_nested_data,index_col).rdd.map(lambda p1:is_json(p1[col_nested_data],p1[index_col],index_col)).map(lambda g2: dict((k.lower(), unicode(v)) if type(v) != "unicode" else ((k.lower(), v)) for k, v in g2.iteritems())).map(lambda g4: dict((k29,unicode("")) if k29 not in g4.keys() else (k29,g4[k29]) for k29 in df2_1_col))
    df3_2=df2_1.map(lambda v:Row(**v)).toDF()
    #df2=df2.withColumn("columnindex",  row_number().over(Window().partitionBy(lit("A")).orderBy(lit('A'))))
    #df3_2=df3_2.withColumn("columnindex", row_number().over(Window().partitionBy(lit("A")).orderBy(lit('A'))))
    final=df2.join(df3_2, on=index_col,how='inner').drop(df3_2.columnindex)
    final=final.drop(index_col)
    sha_columns=df3_2.columns
    sha_columns.remove(index_col)
    if type(cols_to_add_hash)==list:
        sha_columns.extend(cols_to_add_hash)
    else:
        sha_columns.append(col_nested_data)
    #print(sha_columns)
    final=final.withColumn("sha_key", sha2(concat_ws("||", *sha_columns), 256))
    final=final.withColumn("sha_key2", concat_ws("||", *sha_columns))
    return(final)

sqlContext.clearCache()
sqlContext.sql("use "+db)
tables_in_db=sqlContext.tables(db).select("tableName").rdd.map(lambda l321: l321["tableName"]).collect()

#reads a json, if it is not readable, then all the json is captured in _corrupt_record column, if it is null=>no errors for that message
df_1=sqlContext.read.json(db_location+db+"/raw/JSON_INPUT").cache()
#df_1=sqlContext.read.json("JSON_INPUT_init").cache()
Raw_jsons=df_1.count()
if Raw_jsons==0:
    print("\n\nNo data in the process ids")
    ids_2_load_df.write.mode("append").insertInto("latluri1.fsm_audit")
    exit()


df_1=df_1.sort("entitylogicalname")
df=df_1.withColumn("columnindex",  row_number().over(Window().partitionBy(lit("A")).orderBy(lit('A')))).cache()
df=df.withColumn("Process__id", lit(process__id))
df=df.withColumn("gdia_load_date", lit(str(today_full)))
df=df.withColumnRenamed('messageid','message__id')
df=filter_df(from_df=df,table="bad_records",deciding_col_rename="entitystring",condition="not Null",reason="JSON parsing issues audit columns",list_col_dump_df=["entitystring","process__id","gdia_load_date","reason"],deciding_col="_corrupt_record",drop_dec_col_source=True)
df=df.withColumn("entitystring", to_json(struct(df.columns)))
###################
#QC

if df.groupBy("columnindex").count().filter(col("count")>1).count()>0:
    print("\n\nQC failed, check the code for column index issues")
    exit()

###################
#https://stackoverflow.com/questions/31669308/how-to-split-a-dataframe-into-dataframes-with-same-column-values
#from itertools import chain
#tab_in_json = chain(*df.select("entitylogicalname").distinct().collect())
#df_by_table = {table: df.where(col("entitylogicalname").eqNullSafe(table)) for table in tab_in_json}
###################

tables_2_load=df.select("entitylogicalname").distinct().rdd.map(lambda l494: l494["entitylogicalname"]).filter(lambda l509: l509 is not None).collect()


load_report=df.groupBy("entitylogicalname").count()
load_report=load_report.withColumn("entitylogicalname", lower(col("entitylogicalname")))
#print(load_report.show(10000,False))

for i344_1 in tables_2_load:
#i344_1="gcct_genericInformationtopic"
#if i344_1==i344_1:
    i344=i344_1.lower()
    #bad_records=bad_records.cache()
    #duplicate_records=duplicate_records.cache()    
    print("\n\nTable: "+i344)
    final=extract_nested_json(source_dataframe=df,table=i344_1,col_table_name="entitylogicalname",col_nested_data="entitydata",cols_to_add_hash="eventtype",index_col="columnindex")#.cache()
    print("\nInitial count for table "+i344+":"+ str(final.count()))
    final=filter_df(from_df=final,table="bad_records",deciding_col_rename="temp_col",condition="not Null",reason="JSON parsing issue entity data",list_col_dump_df=["entitystring","process__id","gdia_load_date","reason"],deciding_col="_corrupt_record_data",drop_dec_col_source=True)
    final=final.drop('entitydata')
    print("\nCount for table "+i344+" after removing bad records level 2:"+ str(final.count()))
    if final.count()>0:
	###################Null values in JSON are stored as None in DSC
        ##for i1004 in final.columns:
        ##    final=final.withColumn(i1004, when(final[i1004]=='None',lit(unicode(''))).otherwise(final[i1004]))
        final=final.withColumn('dupeCount2', row_number().over(Window().partitionBy('sha_key').orderBy('sha_key')))
        final=final.withColumn('dupeCount', when(col('dupeCount2') == 1, lit(None)).otherwise(lit("True")))
        final=final.drop('dupeCount2')
################################### prints "duplicates within the batch all the time"
        final=filter_df(from_df=final,table="duplicate_records",deciding_col_rename="temp_col",condition="not Null",reason="Duplicates within the batch",list_col_dump_df=["entitystring","process__id","gdia_load_date","reason","message__id","entitylogicalname","sha_key","sha_key2"],deciding_col="dupeCount",drop_dec_col_source=True)
	print("\nCount for table "+i344+" after removing duplicates level 1:"+ str(final.count()))
        if i344.lower() in tables_in_db:
            	#print("Table already existing: "+i344)
############################################################################################################################
            	sha_key_existing=sqlContext.table(i344).select("sha_key").distinct().withColumnRenamed('sha_key','sha_key_loaded')
		#final_shakeys_2_load=final.select("sha_key").subtract(sha_key_existing).withColumnRenamed('sha_key','sha_key_2load')
            	#final=final.join(final_shakeys_2_load,how='left',on=final.sha_key==final_shakeys_2_load.sha_key_2load)
            	final=final.join(sha_key_existing,how='left',on=final.sha_key==sha_key_existing.sha_key_loaded)
	        #final=final.withColumn('sha_key_2load', when(col('sha_key_2load').isNull(), lit("True")).otherwise(lit(None)))
            	final=filter_df(from_df=final,table="duplicate_records",deciding_col_rename="temp_col",condition="not Null",reason="Duplicates accross the table",list_col_dump_df=["entitystring","process__id","gdia_load_date","reason","message__id","entitylogicalname","sha_key","sha_key2"],deciding_col="sha_key_loaded",drop_dec_col_source=True)
		print("\nCount for table "+i344+" after removing duplicates level 2:"+ str(final.count()))
############################################################################################################################
            #sha_key_in_JSON=final.select("sha_key").rdd.map(lambda l223: l223["sha_key"]).collect()
            #sha_key_already_in_table=sqlContext.table(i344).select("sha_key").rdd.map(lambda l226: l226["sha_key"] ).filter(lambda l666: l666 in sha_key_in_JSON).distinct().collect()
            #final_shakeys_2_load_list=list(set(sha_key_in_JSON).difference(set(sha_key_already_in_table)))
            #final_shakeys_2_load=sqlContext.createDataFrame(sc.parallelize(final_shakeys_2_load_list).map(lambda x71: Row(x71)),['sha_key_2load'])
            #final.join(final_shakeys_2_load,final.sha_key==final_shakeys_2_load.sha_key_2load).withColumn('sha_key_2load', when(col('sha_key_2load').isNull(), lit("True")).otherwise(lit(None)))
            #final,duplicate_records=update_df(final,duplicate_records,"temp_col","Duplicates accross the table",["entitystring","process__id","gdia_load_date","reason","sha_key"],"sha_key_2load")
            	if final.count()>0:
			final=final.drop('entitystring')
	            	final=add_columns(final,table=i344)
        	    	create_update_schema(final,db_location,db,table=i344)
        	    	final.write.mode("append").format("com.databricks.spark.avro").insertInto(db+"."+i344)
        	    	print("\nLOADED :"+str(i344)+":"+str(final.count()))
		else:
			print("No data to load in table "+i344)
        else:
		if final.count()>0:
	            	final=final.drop('entitystring')
        	    	hive_create_new_table(final,db_location,db,i344)
        	    	print("\nLOADED :"+str(i344)+":"+str(final.count()))
		else:
			print("No data to load in table "+i344)
    else:
        print("INFO: No Data to load the table "+i344+". Please Investigate for ALL bad records")
    try:
	del final
    	del df2
    	del df3_2
    	del df2_1
    except:
	pass
    


#Final check
m=0
print("\n\nThe following tables are loaded with process__id as "+process__id+" and gdia_load_date "+today_full)
load_list=[]
tables_2_load.append("bad_records")
tables_2_load.append("duplicate_records")
for j_1 in tables_2_load:
        load_report_after={}
        j=j_1.lower()
        try:
                k=sqlContext.table(j).where("process__id='"+process__id+"'").count()
                #print (str(j)+":"+str(k))
                m=m+k
                load_report_after["entitylogicalname"]=j
                load_report_after["count_loaded"]=k
                load_list.append(load_report_after)
        except:
                load_report_after["entitylogicalname"]=j
                load_report_after["count_loaded"]=0
                load_list.append(load_report_after)



load_report_loaded=sc.parallelize(load_list).map(lambda v:Row(**v)).toDF()
load_report=load_report.join(load_report_loaded,on="entitylogicalname",how='outer')
load_report=load_report.withColumn("difference", col("count")-col("count_loaded"))
l2788=load_report.agg({"difference":"sum"}).collect()[0]["sum(difference)"]
if int(Raw_jsons)==int(m)+int(l2788):
	print(str(m)+" Jsons loaded into tables successfully.")
    	ids_2_load_df.write.mode("append").insertInto("latluri1.fsm_audit")
else:
    	print("Read "+str(Raw_jsons)+" and loaded "+str(m)+". Please investigate further. Search the data by  "+process__id+" and gdia_load_date:"+today_full)



load_report.show(10000,False)
