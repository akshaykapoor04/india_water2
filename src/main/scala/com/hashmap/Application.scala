package com.hashmap

import java.io.FileNotFoundException

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import spark_wrapper.SparkWrapper
import utils.Util

object Application {
  def main(args: Array[String]): Unit = {
    try {
      val spark = new SparkWrapper().getSparkObject("indiawater", "yarn")
      val indiaWaterIntial = loadData("hdfs://sandbox-hdp.hortonworks.com:8020//IndiaAffectedWaterQualityAreas.csv" ,spark)
//      "hdfs://sandbox-hdp.hortonworks.com:8020//IndiaAffectedWaterQualityAreas.csv",
//      "hdfs://sandbox-hdp.hortonworks.com:8020//Districts_Codes_2001.csv"
      val districtCodeIntial = loadData("hdfs://sandbox-hdp.hortonworks.com:8020//Districts_Codes_2001.csv", spark)
      val cleanedWaterData = getCleanedWaterData(indiaWaterIntial)
      val cleanedDistrictData = getCleanedDistrictData(districtCodeIntial)
      val joinedDataFrame = getJoinedDataFrame(cleanedWaterData, cleanedDistrictData)
      val frequencyData = getFrequencyData(joinedDataFrame)
      val lodedTotal=loadTotalDataToHive(joinedDataFrame,spark)
      frequencyData.show()
      val loaded = loadDataToHive(frequencyData, spark)
    }catch {
      case e:Exception=>e.printStackTrace()
    }
  }
  def loadData(filePath:String,spark:SparkSession):DataFrame={
    try {
      val intialDataFrame = spark.read.option("inferSchema", true).option("header", true).csv(filePath)
      intialDataFrame
    }catch {
      case ex:FileNotFoundException=>throw new FileNotFoundException(filePath +" file not found")
      case ex:Exception=>throw new Exception("Exception in loadData")
    }
  }

  def getJoinedDataFrame(cleanedWaterData:DataFrame,cleanedDistrictData:DataFrame):DataFrame={
    val joinedDataFrame=cleanedDistrictData.join(cleanedWaterData,cleanedWaterData.col("district_name")===cleanedDistrictData.col("district"),"inner")
    val cleanedDataFrame=joinedDataFrame.select(col("state_code"),
      col("district_code"),
      col("state_name"),
      col("district_name"),
      col("block_name"),
      col("panchayat_name"),
      col("village_name"),
      col("habitation_name"),
      col("quality_parameter"),
      col("year")
    )
    cleanedDataFrame
  }

  def getCleanedWaterData(dataFrame: DataFrame):DataFrame={
    val cleanedWaterData=dataFrame.select(Util.convertToLowerCase(Util.removeCodes(col("State Name"))).alias("state_name"),
      Util.convertToLowerCase(Util.removeCodes(col("District Name"))).alias("district_name"),
      Util.convertToLowerCase(Util.removeCodes(col("Block Name"))).alias("block_name"),
      Util.convertToLowerCase(Util.removeCodes(col("Panchayat Name"))).alias("panchayat_name"),
      Util.convertToLowerCase(Util.removeCodes(col("Village Name"))).alias("village_name"),
      Util.convertToLowerCase(Util.removeCodes(col("Habitation Name"))).alias("habitation_name"),
      col("Quality Parameter").alias("quality_parameter"),
      Util.getYear(col("Year")).alias("year").cast("integer")
    )
  cleanedWaterData
  }

  def getCleanedDistrictData(dataFrame: DataFrame):DataFrame={
    val filteredData=dataFrame.filter(!col("Name of the State/Union territory and Districts").contains("*"))
    val cleanedDistrictData=filteredData.select(col("State Code").alias("state_code"),
      Util.changeDistrictCode(col("District Code")).alias("district_code"),
      Util.changeSpecial(Util.convertToLowerCase(Util.removeCodes(col("Name of the State/Union territory and Districts")))).alias("district")
    )
    cleanedDistrictData
  }
  def getFrequencyData(dataFrame: DataFrame):DataFrame={
    val frequencyData=dataFrame.groupBy("village_name","quality_parameter","year").count()
    val finaldata=frequencyData.select(
      col("village_name").alias("village_name"),
      col("quality_parameter").alias("quality_parameter"),
      col("year"),
      col("count").alias("frequency")
    )
      finaldata
  }
  def loadDataToHive(dataFrame: DataFrame,sparkSession: SparkSession):Boolean={
    dataFrame.write.format("orc").mode(SaveMode.Append).saveAsTable("water_data"+"."+"indian_village_water_data")
    true
  }
  def loadTotalDataToHive(dataFrame: DataFrame,sparkSession: SparkSession):Boolean={
    dataFrame.write.format("orc").mode(SaveMode.Append).saveAsTable("water_data"+"."+"indian_village_water_data_total")
    true
  }
}
