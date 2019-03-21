/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
                          createDisposition: CreateDisposition.Value,
                          tmpWriteOptions: Map[String, String]): Unit = {
    val bucket = conf.get(BigQueryConfiguration.GCS_BUCKET_KEY)
    val temp = s"spark-bigquery-${System.currentTimeMillis()}=${Random.nextInt(Int.MaxValue)}"
    val gcsPath = s"gs://$bucket/hadoop/tmp/spark-bigquery/$temp"

    tmpWriteOptions match {
      case null => df.write.format("avro").save(gcsPath)
      case _ => df.write.options(tmpWriteOptions).format("avro").save(gcsPath)
    }

    val fdf = bq.load(gcsPath, tableRef, writeDisposition, createDisposition)
    delete(new Path(gcsPath))
    fdf
  }

  /**
    * Save a [[DataFrame]] to a BigQuery table.
    */
  def saveAsBigQueryTable(tableSpec: String,
                          writeDisposition: WriteDisposition.Value = null,
                          createDisposition: CreateDisposition.Value = null,
                          tmpWriteOptions: Map[String,String] = null): Unit =
    saveAsBigQueryTable(
      BigQueryStrings.parseTableReference(tableSpec),
      writeDisposition,
      createDisposition,
      tmpWriteOptions)

  private def delete(path: Path): Unit = {
    val fs = FileSystem.get(path.toUri, conf)
    fs.delete(path, true)
  }
}
