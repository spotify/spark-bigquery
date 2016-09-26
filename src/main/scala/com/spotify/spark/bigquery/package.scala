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

package com.spotify.spark

import com.databricks.spark.avro._
import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.cloud.hadoop.io.bigquery._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.Random

package object bigquery {

  object CreateDisposition extends Enumeration {
    val CREATE_IF_NEEDED, CREATE_NEVER = Value
  }

  object WriteDisposition extends Enumeration {
    val WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY = Value
  }

  /**
   * Enhanced version of [[SQLContext]] with BigQuery support.
   */
  implicit class BigQuerySQLContext(self: SQLContext) {

    val sc = self.sparkContext
    val conf = sc.hadoopConfiguration
    val bq = BigQueryClient.getInstance(conf)

    // Register GCS implementation
    if (conf.get("fs.gs.impl") == null) {
      conf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
    }

    /**
     * Set GCP project ID for BigQuery.
     */
    def setBigQueryProjectId(projectId: String): Unit = {
      conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)

      // Also set project ID for GCS connector
      if (conf.get("fs.gs.project.id") == null) {
        conf.set("fs.gs.project.id", projectId)
      }
    }

    /**
     * Set GCS bucket for temporary BigQuery files.
     */
    def setBigQueryGcsBucket(gcsBucket: String): Unit =
      conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)

    /**
     * Set BigQuery dataset location, e.g. US, EU.
     */
    def setBigQueryDatasetLocation(location: String): Unit =
      conf.set(BigQueryClient.STAGING_DATASET_LOCATION, location)

    /**
     * Set GCP JSON key file.
     */
    def setGcpJsonKeyFile(jsonKeyFile: String): Unit = {
      conf.set("mapred.bq.auth.service.account.json.keyfile", jsonKeyFile)
      conf.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFile)
    }

    /**
     * Set GCP pk12 key file.
     */
    def setGcpPk12KeyFile(pk12KeyFile: String): Unit = {
      conf.set("google.cloud.auth.service.account.keyfile", pk12KeyFile)
      conf.set("mapred.bq.auth.service.account.keyfile", pk12KeyFile)
      conf.set("fs.gs.auth.service.account.keyfile", pk12KeyFile)
    }

    /**
     * Perform a BigQuery SELECT query and load results as a [[DataFrame]].
     * @param sqlQuery SQL query in SQL-2011 dialect.
     */
    def bigQuerySelect(sqlQuery: String): DataFrame = bigQueryTable(bq.query(sqlQuery))

    /**
     * Load a BigQuery table as a [[DataFrame]].
     */
    def bigQueryTable(tableRef: TableReference): DataFrame = {
      conf.setClass(
        AbstractBigQueryInputFormat.INPUT_FORMAT_CLASS_KEY,
        classOf[AvroBigQueryInputFormat], classOf[InputFormat[LongWritable, GenericData.Record]])

      BigQueryConfiguration.configureBigQueryInput(
        conf, tableRef.getProjectId, tableRef.getDatasetId, tableRef.getTableId)

      val fClass = classOf[AvroBigQueryInputFormat]
      val kClass = classOf[LongWritable]
      val vClass = classOf[GenericData.Record]
      val rdd = sc
        .newAPIHadoopRDD(conf, fClass, kClass, vClass)
        .map(_._2)
      val schemaString = rdd.map(_.getSchema.toString).first()
      val schema = new Schema.Parser().parse(schemaString)

      val structType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
      val converter = SchemaConverters.createConverterToSQL(schema)
        .asInstanceOf[GenericData.Record => Row]
      self.createDataFrame(rdd.map(converter), structType)
    }

    /**
     * Load a BigQuery table as a [[DataFrame]].
     */
    def bigQueryTable(tableSpec: String): DataFrame =
      bigQueryTable(BigQueryStrings.parseTableReference(tableSpec))

  }

  /**
   * Enhanced version of [[DataFrame]] with BigQuery support.
   */
  implicit class BigQueryDataFrame(self: DataFrame) {

    val sqlContext = self.sqlContext
    val conf = sqlContext.sparkContext.hadoopConfiguration
    val bq = BigQueryClient.getInstance(conf)

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
      self.write.avro(gcsPath)
      val df = bq.load(gcsPath, tableRef, writeDisposition, createDisposition)
      delete(new Path(gcsPath))
      df
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

}
