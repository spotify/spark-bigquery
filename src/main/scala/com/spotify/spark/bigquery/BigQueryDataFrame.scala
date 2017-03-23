package com.spotify.spark.bigquery

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryStrings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.databricks.spark.avro._

import scala.util.Random

object CreateDisposition extends Enumeration {
  val CREATE_IF_NEEDED, CREATE_NEVER = Value
}

object WriteDisposition extends Enumeration {
  val WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY = Value
}


class BigQueryDataFrame(df: DataFrame) {
  val sqlContext: SQLContext = df.sqlContext
  val conf: Configuration = sqlContext.sparkContext.hadoopConfiguration
  val bq: BigQueryClient = BigQueryClient.getInstance(conf)

  sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")

  /**
    * Save a [[DataFrame]] to a BigQuery table.
    */
  def saveAsBigQueryTable(tableRef: TableReference,
                          writeDisposition: WriteDisposition.Value,
                          createDisposition: CreateDisposition.Value): Unit = {
    val bucket = conf.get(BigQueryConfiguration.GCS_BUCKET_KEY)
    val temp = s"spark-bigquery-${System.currentTimeMillis()}=${Random.nextInt(Int.MaxValue)}"
    val gcsPath = s"gs://$bucket/hadoop/tmp/spark-bigquery/$temp"
    df.write.avro(gcsPath)

    val fdf = bq.load(gcsPath, tableRef, writeDisposition, createDisposition)
    delete(new Path(gcsPath))
    fdf
  }

  /**
    * Save a [[DataFrame]] to a BigQuery table.
    */
  def saveAsBigQueryTable(tableSpec: String,
                          writeDisposition: WriteDisposition.Value = null,
                          createDisposition: CreateDisposition.Value = null): Unit =
    saveAsBigQueryTable(
      BigQueryStrings.parseTableReference(tableSpec),
      writeDisposition,
      createDisposition)

  private def delete(path: Path): Unit = {
    val fs = FileSystem.get(path.toUri, conf)
    fs.delete(path, true)
  }
}
