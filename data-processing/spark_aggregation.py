import os
from pyspark.sql import Row, SparkSession, SQLContext
from enum import Enum
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import *
import psycopg2
import pyspark
import itertools
from datetime import datetime, timedelta
from configs import db_config,aws_config
import requests 
from bs4 import BeautifulSoup 
from entity_codes import category_names

#connect to db and get unprocessed files
def get_unprocessed_files_from_DB(connection):
    cursor = connection.cursor()
    query = "SELECT distinct file_name FROM log_filename WHERE Isdownloaded = 1 AND Isprocessed =0 AND file_name LIKE '%export%' AND file_name not LIKE '%2018%'"
    cursor.execute(query)
    results = cursor.fetchall()
    filenames =[]
    for i in results:
        filenames.append(i[0])
    return filenames

def event_exists(r):
   if r in category_names:
       return r
   return ''

def getTopTenEvents (t):
    country,date = t[0]
    listCategoryMentionsURL, listCategoryScoreMap = t[1]
    categoryTopEventsMap = {}
    urlCategoryToEventMap = {}
    # each event stored as (event_id, mentions, url)
    for categoryMention in listCategoryMentionsURL:
        category, numMentions, url, eventID = categoryMention
        if category not in categoryTopEventsMap:
            categoryTopEventsMap[category] = []
        # combine events with same SOURCEURL values
        if (url, category) in urlCategoryToEventMap:
            event_map = urlCategoryToEventMap[(url, category)]
            event_map['numMentions'] += numMentions
        else:
            event_map = {'eventID': eventID, 'numMentions': numMentions, 'url': url}
            categoryTopEventsMap[category].append(event_map)
            urlCategoryToEventMap[(url, category)] = event_map
        categoryTopEventsMap[category] = sorted(categoryTopEventsMap[category], key=lambda event_map: event_map['numMentions'], reverse=True)
        # if (len(categoryTopEventsMap[category]) > 10):
        #     categoryTopEventsMap[category].pop()
    return (t[0], t[1], categoryTopEventsMap)

def aggregateScores (t):
    country,date = t[0]
    listCategoryMentionsURL, listCategoryScoreMap = t[1]
    avgCategoryScoreMap = {}
    countCategoryMap = {}
    for categoryScoreMap in listCategoryScoreMap:
        category, scoreMap = categoryScoreMap
        if category not in countCategoryMap:
            countCategoryMap[category] = 0
        countCategoryMap[category] += 1
        if category not in avgCategoryScoreMap:
            avgCategoryScoreMap[category] = {}
            avgCategoryScoreMap[category]['gs'] = 0
            avgCategoryScoreMap[category]['tone'] = 0
        # calculate average of GoldsteinScale
        if scoreMap['gs']:
            avgCategoryScoreMap[category]['gs'] = ( \
                avgCategoryScoreMap[category]['gs'] * (countCategoryMap[category] - 1) + \
                scoreMap['gs'])/countCategoryMap[category]
        # calculate average of Tone
        if scoreMap['tone']:
            avgCategoryScoreMap[category]['tone'] = ( \
                avgCategoryScoreMap[category]['tone'] * (countCategoryMap[category] - 1) + \
                scoreMap['tone'])/countCategoryMap[category]
    return (t[0], t[1], t[2], avgCategoryScoreMap)

def prepareToInsert (t):
    return (t[0][0], t[0][1], t[3], t[2])

def send_event_to_SQL(rdd,connection,table_name,column_name, file_date):
    #connect to db 
    cursor = connection.cursor()
    for line in rdd.collect():
        query = 'INSERT INTO {} ({}) VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(table_name,column_name)
        country = line[0]
        year = line[1]
        category_dict = {}
        for key, values in line[2].items():
            if key not in category_dict:
                category_dict[key] = {}
            for k2, v2 in values.items():
                category_dict[key][k2] = v2

        for key, values in line[3].items():
            if key not in category_dict:
                category_dict[key] = {}
            for k2, v2 in values[0].items():
                category_dict[key][k2] = v2                

        for key, values in category_dict.items():
            page = parse_url(values['url'])
            data = (country, year, key, values['numMentions'],values['url'],values['eventID'],values['gs'],values['tone'],file_date, page['title'],page['description'],page['image_url'])
            cursor.execute(query, data)
            connection.commit()

def processDataForTimePeriod(master_event_today,file_date):
    timePeriodCassandraRDD = master_event_today.rdd \
            .map(lambda a: ( \
                (a["country"], a["date"]),
                (
                    [(a["actor_type"], a["num_mentions"], a["mention_source"], a["event_id"])], # [("GOV", 30, "http://bbc/article123")]   [("CVL", 50, "http://cnn/article456")]
                    [(a["actor_type"], {"gs": a["goldstein_scale"], "tone": a["tone"]})] \
                ) \
            )) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .map(getTopTenEvents) \
            .map(aggregateScores) \
            .map(prepareToInsert)
 
    table_name ='public.category_events'
    column_name ='country,cyear,actor_type,numMentions,mention_source,event_id,gs,tone,file_date,page_title,page_description,page_image_url'
    send_event_to_SQL(timePeriodCassandraRDD,connection,table_name,column_name,file_date)

def parse_url(url):   
    page = {
        'description': 'None',
        'title': 'None',
        'image_url': 'None'
    }

    try:
        r = requests.get(url, timeout=3) 
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, 'html.parser') 

            title = soup.find("meta",  property="og:title")
            description = soup.find("meta",  property="og:description")
            image = soup.find("meta",  property="og:image")

            if title:
                page['title'] = title['content']

            if description:
                page['description'] = description['content']

            if image:
                page['image_url'] = image['content']

    except Exception as e:
        print(e)

    return page
    
#connect to db and update the log table that files were processed
def update_log_files_SQL(connection,file_name):
    print(file_name)
    cursor = connection.cursor()
    cursor.execute("update log_filename set Isprocessed = 1 where file_name = '{}'".format(file_name))
    connection.commit()

if __name__ == '__main__':
    # Set configurations
    AWS_ACCESS_KEY = aws_config['AWS_ACCESS_KEY']
    AWS_SECRET_ACCESS_KEY = aws_config['AWS_SECRET_ACCESS_KEY']
    connection = psycopg2.connect(host = db_config['host'], database = db_config['database'], user = db_config['username'], password = db_config['password'])
   
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    APP_NAME = "GDELT"
    conf = SparkConf().setAppName(APP_NAME)
    spark = SparkSession.builder.appName("APP_NAME") \
        .config("spark.executor.memory", "1gb") \
        .getOrCreate()

    # Set HDFS configurations
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
    sqlContext.setConf("spark.sql.parquet.compression.code","gzip")
    file_names = get_unprocessed_files_from_DB(connection)
    category_exists = udf(event_exists,StringType())
    for file_name in file_names:
        file_date = file_name.split('.')[0]
        event_today = spark.read.parquet("s3a://gdelt-streaming-events/{}".format(file_name))
        event_today.createOrReplaceTempView("event_today")
        
        #CREATING TODAY'S MASTER DATAFRAME
        master_event_today = spark.sql("SELECT CAST(SQLDATE AS INTEGER) as date, \
                                                CAST(MonthYear AS INTEGER) as month,\
                                                CAST(Year AS INTEGER) as year,\
                                                ActionGeo_CountryCode as country, \
                                                CASE WHEN Actor1Type1Code = '' AND Actor1Type2Code <> '' THEN Actor1Type2Code \
                                                ELSE Actor1Type1Code END AS actor_type, \
                                                CAST(GoldsteinScale AS float) as goldstein_scale, \
                                                CAST(NumMentions AS INTEGER) as num_mentions, \
                                                CAST(AvgTone AS float) as tone, \
                                                SOURCEURL as mention_source, \
                                                GLOBALEVENTID as event_id \
                                            FROM event_today ")
                                                                              
        master_event_today =  master_event_today \
                .filter("country != ''") \
                .withColumn("actor_type",category_exists(col("actor_type"))) \
                .filter("actor_type != ''") \
                .filter("mention_source != ''") 
        
        processDataForTimePeriod(master_event_today,file_date)
        update_log_files_SQL(connection,file_name)
       