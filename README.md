# AWS-Lambda-TLQ-Pipeline
AWS Serverless TLQ(Transform-Load-Query) Pipeline

This project implements a multi-stage TLQ pipeline as a set of AWS Lambda services. This TLQ pipeline is very similar to the
ETL pipeline except that transform phase additionally incorporates the extract. Service #1 performs Extract and Transform,
Service #2 performs Load and Service #3 performs Query.
