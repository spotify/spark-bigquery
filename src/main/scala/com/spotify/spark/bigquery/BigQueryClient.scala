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

import java.util.UUID

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.cloud.hadoop.io.bigquery._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Progressable
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.NonFatal

private[bigquery] class BigQueryClient(conf: Configuration) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BigQueryClient])

  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava
  private val STAGING_DATASET_PREFIX = "bq.staging_dataset.prefix"
  private val STAGING_DATASET_PREFIX_DEFAULT = "spark_bigquery_staging_"
  private val STAGING_DATASET_TABLE_EXPIRATION_MS = 86400000L
  private val STAGING_DATASET_DESCRIPTION = "Spark BigQuery staging dataset"

  private val projectId: String = conf.get(BigQueryConfiguration.PROJECT_ID_KEY)
  private val bigquery: Bigquery = {
    val credential = GoogleCredential.getApplicationDefault.createScoped(SCOPES)
    new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, credential)
      .setApplicationName("spark-bigquery")
      .build()
  }

  private def inConsole = Thread.currentThread().getStackTrace.exists(
    _.getClassName.startsWith("scala.tools.nsc.interpreter."))
  private val PRIORITY = if (inConsole) "INTERACTIVE" else "BATCH"
  private val TABLE_ID_PREFIX = "spark_bigquery"
  private val JOB_ID_PREFIX = "spark_bigquery"
  private val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  /**
   * Perform a BigQuery SELECT query and save results to a temporary table.
   */
  def query(sqlQuery: String): TableReference = {
    val location = getQueryLocation(sqlQuery)
    val destinationTable = temporaryTable(location)
    val job = createQueryJob(sqlQuery, destinationTable, dryRun = false)
    waitForJob(job)
    destinationTable
  }

  /**
   * Load an Avro data set on GCS to a BigQuery table.
   */
  def load(gcsPath: String, destinationTable: TableReference,
           writeDisposition: WriteDisposition.Value = null,
           createDisposition: CreateDisposition.Value = null): Unit = {
    var loadConfig = new JobConfigurationLoad()
      .setDestinationTable(destinationTable)
      .setSourceFormat("AVRO")
      .setSourceUris(List(gcsPath + "/*.avro").asJava)
    if (writeDisposition != null) {
      loadConfig = loadConfig.setWriteDisposition(writeDisposition.toString)
    }
    if (createDisposition != null) {
      loadConfig = loadConfig.setCreateDisposition(createDisposition.toString)
    }

    val jobConfig = new JobConfiguration().setLoad(loadConfig)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
    waitForJob(job)
  }

  private def waitForJob(job: Job): Unit = {
    BigQueryUtils.waitForJobCompletion(bigquery, projectId, job.getJobReference, new Progressable {
      override def progress(): Unit = {}
    })
  }

  private def getQueryTables(sqlQuery: String): Seq[TableReference] = {
    val job = createQueryJob(sqlQuery, null, dryRun = true)
    job.getStatistics.getQuery.getReferencedTables.asScala
  }

  private def getQueryLocation(sqlQuery: String): String = {
    val s = getQueryTables(sqlQuery).map { t =>
      bigquery.datasets().get(t.getProjectId, t.getDatasetId).execute().getLocation
    }.toSet
    if (s.size == 1) {
      Option(s.head).getOrElse("US")
    } else {
      throw new IllegalArgumentException("Invalid dataset locations: " + s.mkString(", "))
    }
  }

  private def stagingDataset(location: String): DatasetReference = {
    // Create staging dataset if it does not already exist
    val prefix = conf.get(STAGING_DATASET_PREFIX, STAGING_DATASET_PREFIX_DEFAULT)
    val datasetId = prefix + location.toLowerCase
    try {
      bigquery.datasets().get(projectId, datasetId).execute()
      logger.info(s"Staging dataset $projectId:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 =>
        logger.info(s"Creating staging dataset $projectId:$datasetId")
        val dsRef = new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
        val ds = new Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(STAGING_DATASET_TABLE_EXPIRATION_MS)
          .setDescription(STAGING_DATASET_DESCRIPTION)
          .setLocation(location)
        bigquery
          .datasets()
          .insert(projectId, ds)
          .execute()

      case NonFatal(e) => throw e
    }
    new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
  }

  private def temporaryTable(location: String): TableReference = {
    val now = Instant.now().toString(TIME_FORMATTER)
    val tableId = TABLE_ID_PREFIX + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(stagingDataset(location).getDatasetId)
      .setTableId(tableId)
  }

  private def createQueryJob(sqlQuery: String,
                             destinationTable: TableReference,
                             dryRun: Boolean): Job = {
    var queryConfig = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setPriority(PRIORITY)
      .setUseLegacySql(false)
      .setCreateDisposition("CREATE_IF_NEEDED")
      .setWriteDisposition("WRITE_EMPTY")
    if (destinationTable != null) {
      queryConfig = queryConfig.setDestinationTable(destinationTable)
    }

    val jobConfig = new JobConfiguration().setQuery(queryConfig).setDryRun(dryRun)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
  }

  private def createJobReference(projectId: String, jobIdPrefix: String): JobReference = {
    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    new JobReference().setProjectId(projectId).setJobId(fullJobId)
  }

}
