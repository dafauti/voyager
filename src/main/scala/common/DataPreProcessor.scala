
import org.apache.spark.sql.DataFrame

trait DataPreProcessor {

  def readRawData(): DataFrame

  def performNecessaryCleanUp(): DataFrame

  def writeToStagingTables():Unit

}
