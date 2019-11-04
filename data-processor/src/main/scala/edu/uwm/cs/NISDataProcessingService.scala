package edu.uwm.cs

import org.apache.spark.sql.{DataFrame, SparkSession}

class NISDataProcessingService(dataSourcePath: String) extends Serializable {

  lazy val spark: SparkSession = SparkSession.builder
//    .master("local[*]") // remove when submitting to EMR
    .getOrCreate

  val nisDataParser = new NISDataParser

  /**
   * Processes 2016 NIS Core Data
   * @return DataFrame with 99 data elements converted as double type
   */
  def process(): DataFrame = {
    val originalDF = loadData()
    val parsedDF = nisDataParser.parseData(originalDF)
    parsedDF
  }

  /**
   * Saves transformed data to S3 in csv format
   * @param processedDF DataFrame with processed data
   * @param dataOutputPath S3 location to store output
   */
  def saveCsvToS3(processedDF: DataFrame, dataOutputPath: String): Unit = {
    processedDF.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save(dataOutputPath)
  }

  /**
   * Loads original data from given file path
   * @return DataFrame with a single column containing value
   */
  private def loadData(): DataFrame = {
    spark.read
      .format("text")
      .option("header", "false")
      .load(dataSourcePath)
  }
}
