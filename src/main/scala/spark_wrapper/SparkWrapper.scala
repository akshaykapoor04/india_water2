package spark_wrapper

import org.apache.spark.sql.SparkSession

class SparkWrapper {
  def getSparkObject(appName:String,master:String):SparkSession={
    val spark=SparkSession.builder().appName(appName).master(master).enableHiveSupport().getOrCreate()
    spark
  }
}
