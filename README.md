spark-bigquery
==============

Google BigQuery support for Spark, SQL, and DataFrames.

To try it out in a local SBT console:

```scala
import com.spotify.spark.bigquery._

// Set up GCP
val conf = sc.hadoopConfiguration
conf.set("mapred.bq.auth.service.account.json.keyfile", "<JSON_KEY_FILE>")
conf.set("fs.gs.auth.service.account.json.keyfile", "<JSON_KEY_FILE>")
conf.set("fs.gs.project.id", "<BILLING_PROJECT>")

sqlContext.setBigQueryProjectId("<BILLING_PROJECT>")
sqlContext.setBigQueryGcsBucket("<GCS_BUCKET>")

// SQL 2011
// https://cloud.google.com/bigquery/sql-reference/
sqlContext.bigQuerySelect("SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare`")
```

## TODO

- Bypass BigQuery connector for small dataset
- Support writing DataFrames to BigQuery
