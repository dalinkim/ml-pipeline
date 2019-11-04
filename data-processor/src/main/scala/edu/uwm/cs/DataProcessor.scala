package edu.uwm.cs

import org.apache.spark.sql.DataFrame

object DataProcessor {

  private val s3Prefix = "s3://"
  private val s3BucketName = "dalin-ml-pipeline"

  private val dataSourcePath = s"$s3Prefix$s3BucketName" + "/input/*.ASC"
  private val dataOutputPath = s"$s3Prefix$s3BucketName" + "/transformed-csv"

  def main(args: Array[String]): Unit = {

    // using service to handle the main work
    val nisDataProcessingService = new NISDataProcessingService(dataSourcePath)

    // process data
    val processedDF: DataFrame = nisDataProcessingService.process()

    // save csv to S3
    nisDataProcessingService.saveCsvToS3(processedDF, dataOutputPath)
  }
}
