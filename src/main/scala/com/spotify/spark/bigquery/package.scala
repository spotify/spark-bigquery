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

import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.language.implicitConversions

package object bigquery {

  /**
   * Enhanced version of [[SQLContext]] with BigQuery support.
   */
  implicit def makebigQueryContext(sql: SQLContext): BigQuerySQLContext =
    new BigQuerySQLContext(sql)

  /**
   * Enhanced version of [[DataFrame]] with BigQuery support.
   */
  implicit def makebigQueryDataFrame(df: DataFrame): BigQueryDataFrame =
    new BigQueryDataFrame(df)
}
