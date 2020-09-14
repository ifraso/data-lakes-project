# Data Lake Project

## Context

A music streaming startup has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The objective of the project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team to continue finding insights in what songs their users are listening to.

## Description

Files in repo are:

- **dl.cfg**: configuration file that should be filled with AWS credentials (access key ID and secret access key) in order to access S3.
- **etl.py**: Python code that reads JSON data from S3, runs the ETL, and write Parquet data to S3, using Spark.
- **README.md**: this file.

## How to

If you wish to run the code, you shoud:

- be able to launch Spark in local mode
- provide your AWS credentials in *dl.cfg* (admin permissions)
- set up a bucket in your AWS called 'data-lakes-project'
- run *etl.py* script

Note that writing Parquet files to S3 might take a while. At the end you should have the files correctly loaded in your bucket.
