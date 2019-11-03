package edu.uwm.cs

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object DataProcessor {

  private val s3Prefix = "s3://"
  private val s3BucketName = "dalin-ml-pipeline"
  private val indexTablePath = "input/index_table"

  private val dataSourcePath = s"$s3Prefix$s3BucketName" + "/input/NIS_2016_Core.ASC"
  private val diagnosisIndexTablePath = s"$s3Prefix$s3BucketName$indexTablePath" + "/diagnosis_index/*.csv"
  private val externalCauseIndexTablePath = s"$s3Prefix$s3BucketName$indexTablePath" + "/external_cause_index/*.csv"
  private val procedureIndexTablePath = s"$s3Prefix$s3BucketName$indexTablePath" + "/procedure_index/*.csv"
  private val dataOutputPath = s"$s3Prefix$s3BucketName" + "/input/transformed"

  def main(args: Array[String]): Unit = {

    // let NISDataProcessingService do all the work
    val nisDataProcessingService = new NISDataProcessingService(dataSourcePath, diagnosisIndexTablePath, externalCauseIndexTablePath, procedureIndexTablePath)

    // process data
    val processedDF: DataFrame = nisDataProcessingService.process()

    // convert data to libsvm
    val processedRDD: RDD[LabeledPoint] = nisDataProcessingService.toLibsvm(processedDF)

    // save libsvm file in S3
    nisDataProcessingService.saveToS3(processedRDD, dataOutputPath)
  }
}
