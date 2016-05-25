spark-bigquery
==============

[![Build Status](https://travis-ci.org/spotify/spark-bigquery.svg?branch=master)](https://travis-ci.org/spotify/spark-bigquery)
[![GitHub license](https://img.shields.io/github/license/spotify/scio.svg)]()
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/spark-bigquery_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/spark-bigquery_2.11)

Google BigQuery support for Spark, SQL, and DataFrames.

To use the package in a Google [Cloud Dataproc](https://cloud.google.com/dataproc/) cluster:

`spark-shell --packages com.spotify:spark-bigquery_2.10:0.1.0`

To use it in a local SBT console:

```scala
import com.spotify.spark.bigquery._

// Set up GCP credentials
sqlContext.setGcpJsonKeyFile("<JSON_KEY_FILE>")

// Set up BigQuery project and bucket
sqlContext.setBigQueryProjectId("<BILLING_PROJECT>")
sqlContext.setBigQueryGcsBucket("<GCS_BUCKET>")

// Set up BigQuery dataset location, default is US
sqlContext.setBigQueryDatasetLocation("<DATASET_LOCATION>")
```

Usage:

```scala
// Load everything from a table
val table = sqlContext.bigQueryTable("bigquery-public-data:samples.shakespeare")

// Load results from a SQL query
// Only legacy SQL dialect is supported for now
val df = sqlContext.bigQuerySelect(
  "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare]")

  // Save data to a table
df.saveAsBigQueryTable("my-project:my_dataset.my_table")
```

# License

Copyright 2016 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
