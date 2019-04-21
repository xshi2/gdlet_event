# !/bin/bash

/home/ubuntu/.local/bin/spark-submit \
  --jars ~/Documents/Insight/InsightDataEngineer/postgresql-42.2.5.jar \
  --master spark://ec2-52-43-160-56.us-west-2.compute.amazonaws.com:7077 \
  --executor-memory 6G \
  --driver-memory 6G \
  ~/Documents/Insight/InsightDataEngineer/data-processing/spark_aggregation.py


