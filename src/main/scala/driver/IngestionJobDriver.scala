import com.gmail.bigdata.dpp.util.IngestionJobArgumentParser
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import collection.JavaConverters._

object IngestionJobDriver {
  val logger =  Logger.getLogger(this.getClass.getName)
  def main(args: Array[String]): Unit = {
    val argParser = new IngestionJobArgumentParser(args)
    val componentName=argParser.componentName()

    logger.info("Starting DPP Ingestion Job")

    val spark: SparkSession = SparkSession.builder()
      .appName(componentName)
      .enableHiveSupport()
      .getOrCreate()


    val ingestionConfig: Config = ConfigFactory.load()
    val steps=ingestionConfig.getConfigList("query_pipeline.steps").asScala
   // import scala.collection.JavaConversions._
    val stepsList=steps.map(conf=>Steps(conf.getString("step_desc"),conf.getString("output_table_type"),conf.getString("output_table"),conf.getString("write_format"),conf.getString("write_mode"),conf.getString("query"))).toList
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    spark.conf.set ("spark.sql.hive.convertMetastoreOrc","false")
    for( step <- stepsList ){
        logger.info("Running <"+step.stepDesc+"> with output table <"+step.outputTable+"> and with table Type <"+step.outputTableType+">and with write format <"+step.writeFormat+"> and write mode <"+step.writeMode+">")
        logger.info("Query : "+step.query)
        if ( step.outputTableType.equalsIgnoreCase("temporary"))
        {
            spark.sql(step.query).createOrReplaceTempView(step.outputTable)
        }
        else
        {
            spark.sql(step.query).write.format(step.writeFormat).mode(writeModeEnum(step.writeMode)).insertInto(step.outputTable)
        }

    }





  }

  def writeModeEnum(writeMode: String) = if (writeMode.equalsIgnoreCase("OVERWRITE")) SaveMode.Overwrite else SaveMode.Append

  case class Steps(stepDesc:String,outputTableType:String,outputTable: String,writeFormat:String,writeMode:String,query:String)



}
