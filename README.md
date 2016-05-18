spark-bigquery
==============

[![Build Status](https://travis-ci.org/spotify/spark-bigquery.svg?branch=master)](https://travis-ci.org/spotify/spark-bigquery)
[![GitHub license](https://img.shields.io/github/license/spotify/scio.svg)]()

Google BigQuery support for Spark, SQL, and DataFrames.

To try it out in a local SBT console:

```scala
import com.spotify.spark.bigquery._

// Set up GCP credentials
val conf = sc.hadoopConfiguration
conf.set("mapred.bq.auth.service.account.json.keyfile", "<JSON_KEY_FILE>")
conf.set("fs.gs.auth.service.account.json.keyfile", "<JSON_KEY_FILE>")

sqlContext.setBigQueryProjectId("<BILLING_PROJECT>")
sqlContext.setBigQueryGcsBucket("<GCS_BUCKET>")

// SQL 2011
// https://cloud.google.com/bigquery/sql-reference/
val df = sqlContext.bigQuerySelect(
  """
    |SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare
  """.stripMargin)

df.saveAsBigQueryTable("my-project:my_dataset.my_table")
```

# License

Copyright 2016 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
