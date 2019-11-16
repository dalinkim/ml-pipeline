package edu.uwm.cs

import org.apache.spark.sql.{DataFrame, SparkSession}

class NISDataProcessingService() extends Serializable {

  lazy val spark: SparkSession = SparkSession.builder
//    .master("local[*]") // remove when submitting to EMR
    .getOrCreate

  val nisDataParser = new NISDataParser

  /**
   * Processes 2016 NIS Core Data
   *
   * @return DataFrame with 98 data elements
   */
  def process(dataSourcePath: String): DataFrame = {
    val originalDF = loadData(dataSourcePath)
    val parsedDF = nisDataParser.parseData(originalDF)
    parsedDF
  }

  /**
   * Saves transformed data to S3 in csv format
   *
   * @param processedDF DataFrame with processed data
   * @param dataOutputFolderPath Folder path in S3 to store output
   */
  def saveCsvToS3(processedDF: DataFrame, dataOutputFolderPath: String): Unit = {
    processedDF.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save(dataOutputFolderPath)
  }

  /**
   * Loads original data from given file path
   *
   * @param dataSourceFilePath File path in S3 to load data
   * @return DataFrame with a single column containing value
   */
  private def loadData(dataSourceFilePath: String): DataFrame = {
    spark.read
      .format("text")
      .option("header", "false")
      .load(dataSourceFilePath)
  }
}
