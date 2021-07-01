# ETL Spark/Hadoop Jobs

There are different generic jobs based on Spark and Hadoop Common libraries to perform typical ELT tasks:

-   [CheckFileExists.scala](modules/hadoopjobs/src/main/scala/etljobs/hadoop/CheckFileExists.scala): checks that certain files are at DFS location. Returns exit code 0 in case files exist and exit code 99 if they do not.
    Airflow `BashOperator` is then using skip-exit-code to mark current task as skipped.
-   [FileToFile.scala](modules/hadoopjobs/src/main/scala/etljobs/hadoop/FileToFile.scala) copies files from one DFS location to another one
-   [FileToDataset.scala](modules/sparkjobs/src/main/scala/etljobs/spark/FileToDataset.scala) loads data from DFS files into Spark format using Spark Batch API. For example Parquet or other formats
-   [FileStreamToDataset.scala](modules/sparkjobs/src/main/scala/etljobs/spark/FileStreamToDataset.scala) is the same as previos job, but using Spark Streaming API.
-   [CheckDataRecieved.scala](modules/sparkjobs/src/main/scala/etljobs/spark/CheckDataRecieved.scala) checks that all required data exist at specific locations.

See execution examples in Airflow DAGs folder:

-   [airflow-dir/src/dags/spark_example.py](airflow-dir/src/dags/spark_example.py)
-   [airflow-dir/src/dags/spark_full_delta.py](airflow-dir/src/dags/spark_full_delta.py)

Supported input/output formats:

-   CSV, JSON, Parquet
-   Delta Lake
