package edu.uwm.cs

import org.apache.spark.sql.DataFrame

object DataProcessor {

  def main(args: Array[String]): Unit = {

    if (args.length < 2)
      throw new IllegalArgumentException("Missing one or more required arguments: dataSourceFilePath and dataOutputFolderPath should be specified")

    val dataSourceFilePath = args(0) // s3://dalin-ml-pipeline/input/*.ASC
    val dataOutputFolderPath = args(1) // s3://dalin-ml-pipeline/transformed-csv

    // using service to handle the main work
    val nisDataProcessingService = new NISDataProcessingService()

    // process data
    val processedDF: DataFrame = nisDataProcessingService.process(dataSourceFilePath)

    // save csv to S3
    nisDataProcessingService.saveCsvToS3(processedDF, dataOutputFolderPath)
  }
}
