# !/bin/bash
spark-submit \
  --jars /home/ubuntu/Documents/Insight/emotions/spark/postgresql-42.2.5.jar \
  --master spark://ec2-52-43-160-56.us-west-2.compute.amazonaws.com:7077 \
  --executor-memory 6G \
  --driver-memory 6G \
  spark_streaming.py

 # modified_gkg_cooccurences_pyspark.py
