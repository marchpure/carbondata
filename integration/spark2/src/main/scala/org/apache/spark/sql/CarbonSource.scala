/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.io.IOException
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.CarbonParserUtil
import org.apache.spark.sql.execution.command.{TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.strategy.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.hive.CarbonMetaStore
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.{CarbonSpark2SqlParser, CarbonSparkSqlParserUtil}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CarbonSparkUtil}
import org.apache.carbondata.streaming.{CarbonStreamException, CarbonStreamingQueryListener, StreamSinkFactory}

/**
 * Carbon relation provider compliant to data source api.
 * Creates carbon relations
 */
class CarbonSource extends CreatableRelationProvider with RelationProvider
  with SchemaRelationProvider with StreamSinkProvider with DataSourceRegister {

  override def shortName(): String = "carbondata"

  // will be called if hive supported create table command is provided
  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    // if path is provided we can directly create Hadoop relation. \
    // Otherwise create datasource relation
    val newParameters = CarbonScalaUtil.getDeserializedParameters(parameters)
    newParameters.get("tablePath") match {
      case Some(path) => CarbonDatasourceHadoopRelation(sqlContext.sparkSession,
        Array(path),
        newParameters,
        None)
      case _ =>
        val options = new CarbonOption(newParameters)
        val tablePath =
          CarbonEnv.getTablePath(options.dbName, options.tableName)(sqlContext.sparkSession)
        CarbonDatasourceHadoopRelation(sqlContext.sparkSession,
          Array(tablePath),
          newParameters,
          None)
    }
  }

  // called by any write operation like INSERT INTO DDL or DataFrame.write API
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    var newParameters = CarbonScalaUtil.getDeserializedParameters(parameters)
    val options = new CarbonOption(newParameters)
    val isExists = CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.tableExists(
      options.tableName, options.dbName)(sqlContext.sparkSession)
    val (doSave, doAppend) = (mode, isExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        CarbonException.analysisException(s"table path already exists.")
      case (SaveMode.Overwrite, true) =>
        newParameters += (("overwrite", "true"))
        (true, false)
      case (SaveMode.Overwrite, false) | (SaveMode.ErrorIfExists, false) =>
        newParameters += (("overwrite", "true"))
        (true, false)
      case (SaveMode.Append, _) =>
        (false, true)
      case (SaveMode.Ignore, exists) =>
        (!exists, false)
    }

    if (doSave) {
      // save data when the save mode is Overwrite.
      new CarbonDataFrameWriter(sqlContext, data).saveAsCarbonFile(
        CaseInsensitiveMap[String](newParameters))
    } else if (doAppend) {
      new CarbonDataFrameWriter(sqlContext, data).appendToCarbonFile(
        CaseInsensitiveMap[String](newParameters))
    }

    createRelation(sqlContext, newParameters, data.schema)
  }

  // called by DDL operation with a USING clause
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      dataSchema: StructType): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    val newParameters =
      CaseInsensitiveMap[String](CarbonScalaUtil.getDeserializedParameters(parameters))
    val dbName: String =
      CarbonEnv.getDatabaseName(newParameters.get("dbName"))(sqlContext.sparkSession)
    val tableOption: Option[String] = newParameters.get("tableName")
    if (tableOption.isEmpty) {
      CarbonException.analysisException("Table creation failed. Table name is not specified")
    }
    val tableName = tableOption.get.toLowerCase()
    if (tableName.contains(" ")) {
      CarbonException.analysisException(
        "Table creation failed. Table name cannot contain blank space")
    }
    val path = getPathForTable(sqlContext.sparkSession, dbName, tableName, newParameters)

    CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(path), newParameters,
      Option(dataSchema))
  }

  private def addLateDecodeOptimization(ss: SparkSession): Unit = {
    if (ss.sessionState.experimentalMethods.extraStrategies.isEmpty) {
      ss.sessionState.experimentalMethods.extraStrategies = Seq(new CarbonLateDecodeStrategy)
      ss.sessionState.experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)
    }
  }

  /**
   * Returns the path of the table
   *
   * @param sparkSession
   * @param dbName
   * @param tableName
   * @return
   */
  private def getPathForTable(sparkSession: SparkSession, dbName: String,
      tableName : String, parameters: Map[String, String]): String = {

    if (StringUtils.isBlank(tableName)) {
      throw new MalformedCarbonCommandException("The Specified Table Name is Blank")
    }
    if (tableName.contains(" ")) {
      throw new MalformedCarbonCommandException("Table Name Should not have spaces ")
    }
    try {
      if (parameters.contains("tablePath")) {
        parameters("tablePath")
      } else {
        val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
        if (carbonTable != null) {
          carbonTable.getTablePath
        } else {
          throw new MalformedCarbonCommandException("Failed to get path for table")
        }
      }
    } catch {
      case ex: Exception =>
        throw new Exception(s"Do not have $dbName and $tableName", ex)
    }
  }

  /**
   * produce a streaming `Sink` for a specific format
   * now it will create a default sink(CarbonAppendableStreamSink) for row format
   */
  override def createSink(sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {

    // check "tablePath" option
    val options = new CarbonOption(parameters)
    val dbName = CarbonEnv.getDatabaseName(options.dbName)(sqlContext.sparkSession)
    val tableName = options.tableName
    if (tableName.contains(" ")) {
      throw new CarbonStreamException("Table creation failed. Table name cannot contain blank " +
                                      "space")
    }
    val sparkSession = sqlContext.sparkSession
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    if (!carbonTable.isStreamingSink) {
      throw new CarbonStreamException(s"Table ${carbonTable.getDatabaseName}." +
                                      s"${carbonTable.getTableName} is not a streaming table")
    }

    // CarbonSession has added CarbonStreamingQueryListener during the initialization.
    // But other SparkSessions didn't, so here will add the listener once.
    if (!"CarbonSession".equals(sparkSession.getClass.getSimpleName)) {
      if (CarbonSource.listenerAdded.get(sparkSession.hashCode()).isEmpty) {
        synchronized {
          if (CarbonSource.listenerAdded.get(sparkSession.hashCode()).isEmpty) {
            sparkSession.streams.addListener(new CarbonStreamingQueryListener(sparkSession))
            CarbonSource.listenerAdded.put(sparkSession.hashCode(), true)
          }
        }
      }
    }

    // create sink
    StreamSinkFactory.createStreamTableSink(
      sqlContext.sparkSession,
      sqlContext.sparkSession.sessionState.newHadoopConf(),
      carbonTable,
      parameters)
  }

}

object CarbonSource {

  private val LOGGER = LogServiceFactory.getLogService(CarbonSource.getClass.getName)

  lazy val listenerAdded = new mutable.HashMap[Int, Boolean]()

  def createTableInfoFromParams(
      parameters: Map[String, String],
      dataSchema: StructType,
      identifier: AbsoluteTableIdentifier,
      query: Option[LogicalPlan],
      sparkSession: SparkSession): TableModel = {
    val sqlParser = new CarbonSpark2SqlParser
    val map = scala.collection.mutable.Map[String, String]()
    parameters.foreach { case (key, value) => map.put(key, value.toLowerCase()) }
    val options = new CarbonOption(parameters)
    val fields = query match {
      case Some(q) =>
        // if query is provided then it is a CTAS flow
        if (sqlParser.getFields(dataSchema).nonEmpty) {
          throw new AnalysisException(
            "Schema cannot be specified in a Create Table As Select (CTAS) statement")
        }
        sqlParser
          .getFields(CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .getSchemaFromUnresolvedRelation(sparkSession, q))
      case None =>
        sqlParser.getFields(dataSchema)
    }
    val bucketFields = sqlParser.getBucketFields(map, fields, options)
    CarbonParserUtil.prepareTableModel(
      ifNotExistPresent = false,
      Option(identifier.getDatabaseName),
      identifier.getTableName,
      fields,
      Nil,
      map,
      bucketFields)
  }

  /**
   * Update spark catalog table with schema information in case of schema storage is hive metastore
   * @param tableDesc
   * @param sparkSession
   * @return
   */
  def updateCatalogTableWithCarbonSchema(
      tableDesc: CatalogTable,
      sparkSession: SparkSession,
      ignoreIfExists: Boolean,
      query: Option[LogicalPlan] = None): CatalogTable = {
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val storageFormat = tableDesc.storage
    val properties = CarbonSparkSqlParserUtil.getProperties(tableDesc)
    if (!properties.contains("carbonSchemaPartsNo")) {
      val (map, tablePath) = updateAndCreateTable(
        sparkSession,
        metaStore,
        properties,
        query,
        tableDesc,
        ignoreIfExists)
      // updating params
      val updatedFormat = CarbonToSparkAdapter
        .getUpdatedStorageFormat(storageFormat, map, tablePath)
      val updatedSchema = if (tableDesc.tableType == CatalogTableType.EXTERNAL) {
        tableDesc.schema
      } else {
        CarbonSparkUtil.updateStruct(tableDesc.schema)
      }
      tableDesc.copy(
        storage = updatedFormat,
        schema = updatedSchema,
        partitionColumnNames = tableDesc.partitionColumnNames.map(_.toLowerCase)
      )
    } else {
      val tableInfo = CarbonUtil.convertGsonToTableInfo(properties.asJava)
      val isTransactionalTable = properties.getOrElse("isTransactional", "true").contains("true")
      tableInfo.setTransactionalTable(isTransactionalTable)
      val isExternal = properties.getOrElse("isExternal", "true").contains("true")
      val updatedTableType = if (isExternal) {
        tableDesc.tableType
      } else {
        CatalogTableType.MANAGED
      }
      if (isTransactionalTable && !metaStore.isReadFromHiveMetaStore) {
        // save to disk
        metaStore.saveToDisk(tableInfo, properties("tablePath"))
        // remove schema string from map as we don't store carbon schema to hive metastore
        val map = CarbonUtil.removeSchemaFromMap(properties.asJava)
        val updatedFormat = storageFormat.copy(properties = map.asScala.toMap)
        tableDesc.copy(storage = updatedFormat, tableType = updatedTableType)
      } else {
        tableDesc.copy(tableType = updatedTableType)
      }
    }
  }

  def updateAndCreateTable(
      sparkSession: SparkSession,
      metaStore: CarbonMetaStore,
      properties: Map[String, String],
      query: Option[LogicalPlan],
      tableDesc: CatalogTable,
      ignoreIfExists: Boolean): (Map[String, String], String) = {
    val tableInfo = CarbonSparkSqlParserUtil.buildTableInfoFromCatalogTable(
      tableDesc, true, sparkSession, query)
    val tableLocation = if (tableDesc.storage.locationUri.isDefined) {
      Some(tableDesc.storage.locationUri.get.toString)
    } else {
      None
    }
    val isExternal = tableDesc.tableType == CatalogTableType.EXTERNAL
    val tablePath = CarbonEnv.createTablePath(
      Some(tableInfo.getDatabaseName),
      tableInfo.getFactTable.getTableName,
      tableInfo.getFactTable.getTableId,
      tableLocation,
      tableDesc.tableType == CatalogTableType.EXTERNAL,
      tableInfo.isTransactionalTable)(sparkSession)
    tableInfo.setTablePath(tablePath)
    CarbonSparkSqlParserUtil.validateTableProperties(tableInfo)
    val schemaEvolutionEntry = new SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvolution.getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    val map = if (!metaStore.isReadFromHiveMetaStore && tableInfo.isTransactionalTable) {
      saveToDisk(metaStore, ignoreIfExists, tableInfo)
      new java.util.HashMap[String, String]()
    } else {
      CarbonUtil.convertToMultiStringMap(tableInfo)
    }
    CarbonSparkSqlParserUtil
      .normalizeProperties(properties)
      .foreach(e => map.put(e._1, e._2))
    map.put("tablepath", tableInfo.getTablePath)
    map.put("path", tableInfo.getTablePath)
    map.put("dbname", tableInfo.getDatabaseName)
    if (map.containsKey("tableName")) {
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
      LOGGER.warn("tableName is not required in options, ignoring it")
    }
    map.put("tableName", tableInfo.getFactTable.getTableName)
    map.put("isTransactional", tableInfo.isTransactionalTable.toString)
    map.put("isExternal", isExternal.toString)
    (map.asScala.toMap, tableInfo.getTablePath)
  }

  private def saveToDisk(
      metaStore: CarbonMetaStore, ignoreIfExists: Boolean, tableInfo: TableInfo): Unit = {
    try {
      metaStore.saveToDisk(tableInfo, tableInfo.getTablePath)
    } catch {
      case ex: IOException if ignoreIfExists =>
        val schemaFile = CarbonTablePath.getMetadataPath(tableInfo.getTablePath) +
                         CarbonCommonConstants.FILE_SEPARATOR + CarbonTablePath.SCHEMA_FILE
        if (FileFactory.isFileExist(schemaFile)) {
          LOGGER.error(ex)
        } else {
          throw ex
        }
      case ex => throw ex
    }
  }
}

/**
 * Code ported from Apache Spark
 * Builds a map in which keys are case insensitive. Input map can be accessed for cases where
 * case-sensitive information is required. The primary constructor is marked private to avoid
 * nested case-insensitive map creation, otherwise the keys in the original map will become
 * case-insensitive in this scenario.
 */
case class CaseInsensitiveMap[T] (originalMap: Map[String, T]) extends Map[String, T]
  with Serializable {

  val keyLowerCasedMap = originalMap.map(kv => kv.copy(_1 = kv._1.toLowerCase(Locale.ROOT)))

  override def get(k: String): Option[T] = keyLowerCasedMap.get(k.toLowerCase(Locale.ROOT))

  override def contains(k: String): Boolean =
    keyLowerCasedMap.contains(k.toLowerCase(Locale.ROOT))

  override def +[B1 >: T](kv: (String, B1)): Map[String, B1] = {
    new CaseInsensitiveMap(originalMap + kv)
  }

  override def iterator: Iterator[(String, T)] = keyLowerCasedMap.iterator

  override def -(key: String): Map[String, T] = {
    new CaseInsensitiveMap(originalMap.filterKeys(!_.equalsIgnoreCase(key)))
  }
}
