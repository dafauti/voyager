package com.gmail.bigdata.dpp.driver

import com.gmail.bigdata.dpp.allocation.AllocationQueryConstructor
import com.gmail.bigdata.dpp.util.AllocatonJobArgumentParser
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer


object AllocationJobDriver {
  val logger = Logger.getLogger(this.getClass.getName)
  val DROP_OUT_TABLE_SUFFIX = "_dropout_tmp"
  val FILE_OUTPUT_FORMAT = "ORC"

  def main(args: Array[String]): Unit = {

    logger.info("Starting Allocation Job")
    val argParser = new AllocatonJobArgumentParser(args)


    val componentName = argParser.componentName()


    logger.info(s"Starting DPP allocation for component : $componentName")

    val spark = SparkSession
      .builder()
      .appName(componentName)
      .enableHiveSupport()
      .getOrCreate()


    val allocationMasterDataset = argParser.allocationMasterDataset()
    logger.info(s"Allocation Master Table name : $allocationMasterDataset")

    val dbName = argParser.dbName()
    logger.info(s"Database Name : $dbName")

    val outputTableName = argParser.allocationTableName()
    logger.info(s"Allocation Output Table name : $outputTableName")


    val masterTableName=dbName+"."+allocationMasterDataset
    logger.info(s"Master table name : $masterTableName")

    val allocationTableName=dbName+"."+outputTableName
    logger.info(s"Allocation Final Output Table name : $allocationTableName")

    val yearRange = argParser.yearRange()
    logger.info(s"Year Range for running allocation job $yearRange")


    spark.sql("SELECT * FROM " + masterTableName + " WHERE fiscal_year IN " + yearRange +"").createOrReplaceTempView(allocationMasterDataset)


    val ingestionConfig: Config = ConfigFactory.load()
    val steps = ingestionConfig.getConfigList("allocation.steps").asScala
    //import scala.collection.JavaConversions._
    val outputPartitionCols = ingestionConfig.optionalStringList("allocation.outputPartitionCols")
    val dropCols = ingestionConfig.optionalStringList("allocation.dropCols")
    val stepsList = steps.map(conf => AllocationSteps(conf.getString("allocation_file"), conf.getString("allocate_by"), conf.getString("l1_group_by"), conf.getString("column_name"), conf.optionalString("l2_group_by"))).toList


    logger.info("Running level 1 allocation query")
    val allocationQueryConstructor = new AllocationQueryConstructor
    val level1AllocationQuery = allocationQueryConstructor.getL1AllocationQuery(allocationMasterDataset, stepsList)
    logger.info("Level 1 allocation query : " + level1AllocationQuery)
    spark.sql(level1AllocationQuery).createOrReplaceTempView(allocationMasterDataset)
    var updatedConfigDataList = new ArrayBuffer[AllocationSteps](stepsList.length)
    logger.info("Running DropOut Queries for level 2 allocation ")


    for (conf <- stepsList) {
      if (!conf.l2_group_by.isEmpty) {
        val dropOutQuery = allocationQueryConstructor.getDropOutQuery(allocationMasterDataset, conf.allocation_file, conf.allocate_by, conf.l1_group_by, conf.l2_group_by.get, yearRange)
        logger.info("Dropout query for allocation file : " + conf.allocation_file + " : " + dropOutQuery)
        logger.info("Drop out table : " + conf.allocation_file + DROP_OUT_TABLE_SUFFIX)

        //newly added
        val allocationTbl=conf.allocation_file.split('.')(1)
        spark.sql(dropOutQuery).createOrReplaceTempView(allocationTbl+DROP_OUT_TABLE_SUFFIX)
        var allocationObj = AllocationSteps(allocationTbl + DROP_OUT_TABLE_SUFFIX, conf.allocate_by, conf.l1_group_by, conf.column_name, conf.l2_group_by)
        //end here

        //spark.sql(dropOutQuery).write.format(FILE_OUTPUT_FORMAT).mode(SaveMode.Overwrite).saveAsTable(conf.allocation_file + DROP_OUT_TABLE_SUFFIX)
        //var allocationObj = AllocationSteps(conf.allocation_file + DROP_OUT_TABLE_SUFFIX, conf.allocate_by, conf.l1_group_by, conf.column_name, conf.l2_group_by)
        updatedConfigDataList += allocationObj
      }
    }


    logger.info("Running level 2 allocation query")
    val level2AllocationQuery = allocationQueryConstructor.getL2AllocationQuery(allocationMasterDataset, updatedConfigDataList.toArray)
    logger.info(s"Level 2 allocation query : $level2AllocationQuery")
    logger.info(s"Running final output to table : $allocationTableName")

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set ("spark.sql.hive.convertMetastoreOrc","false")


    var finalDf=spark.sql(level2AllocationQuery)
    if(!outputPartitionCols.isEmpty){
      finalDf=movePartitionColsToEnd(finalDf, outputPartitionCols.get)
    }

    if(!dropCols.isEmpty){
      val dropColumns=dropCols.get
      finalDf.drop(dropColumns: _*).write.format(FILE_OUTPUT_FORMAT).mode(SaveMode.Overwrite).insertInto(allocationTableName)
    }else{
      finalDf.write.format(FILE_OUTPUT_FORMAT).mode(SaveMode.Overwrite).insertInto(allocationTableName)
    }

   // val reorderedDf = movePartitionColsToEnd(spark.sql(level2AllocationQuery), outputPartitionCols.toList)
    //reorderedDf.drop(dropCols.toList: _*).write.format(FILE_OUTPUT_FORMAT).mode(SaveMode.Overwrite).insertInto(allocationTableName)
    //finalDf.write

    for (conf <- stepsList) {
      spark.sql("DROP TABLE IF EXISTS " + conf.allocation_file + DROP_OUT_TABLE_SUFFIX)
    }
  }


  case class AllocationSteps(allocation_file: String, allocate_by: String, l1_group_by: String, column_name: String, l2_group_by: Option[String])


  /**
   * Bring partition columns to the end to enable dynamic partition while writing
   *
   * @param df
   * @param partitionColumns
   * @return
   */
  def movePartitionColsToEnd(df: DataFrame, partitionColumns: List[String]): DataFrame = {

    val newDf = partitionColumns.foldLeft(df) {
      (tmpdf, c) => tmpdf.withColumn(c + "_dummy", col(c)).drop(c).withColumnRenamed(c + "_dummy", c)
    }
    logger.info("Reordered Columns after moving partition columns " + newDf.schema)
    newDf
  }


  implicit class RichConfig(val config: Config) extends AnyVal {
    import scala.collection.JavaConverters._

    private def getOptional[T](path: String, get: String => T): Option[T] = {
      if (config.hasPath(path)) {
        Some(get(path))
      } else {
        None
      }
    }

    def optionalString(path: String): Option[String] = getOptional(path, config.getString)

    def optionalStringList(path: String): Option[List[String]] = getOptional(path, config.getStringList).map(_.asScala.toList)

  }

}
