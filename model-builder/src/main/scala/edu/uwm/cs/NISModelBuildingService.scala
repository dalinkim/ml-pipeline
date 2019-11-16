package edu.uwm.cs

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}

class NISModelBuildingService(allNumericColumns: Array[String]) extends Serializable {

  lazy val spark: SparkSession = SparkSession.builder
//    .master("local[*]") // remove when submitting to EMR
    .getOrCreate

  /**
   * Extracts data for diagnosis, converts numeric columns to double type, and removes invalid/missing data
   *
   * @param dataSourceFilePath File path in S3 to load data
   * @param diagnosis ICD-10-CM code representing the diagnosis used for this model building
   * @return prepared data ready to go through Spark pipeline
   */
  def prepareData(dataSourceFilePath: String, diagnosis: String): DataFrame = {
    val transformedDF = loadTransformedData(dataSourceFilePath)
    val oneDiagOneProcDF = getOneDiagOneProcData(transformedDF, diagnosis)
    val convertedDF = convertNumericColumnsToDouble(oneDiagOneProcDF)
    val filteredDF = removeInvalidData(convertedDF)
    filteredDF
  }

  /**
   * Trains, builds, and deploys a machine learning model using SageMaker
   *
   * @param preparedDF prepared data ready to go through Spark pipeline
   * @param pipeline Spark pipeline with transformers and SageMaker estimator
   */
  def buildAndDeployModel(preparedDF: DataFrame, pipeline: Pipeline): Unit = {
    pipeline.fit(preparedDF.withColumnRenamed("TOTCHG", "label"))
  }

  /**
   * Loads transformed data from given file path
   *
   * @return transformed data after initial data processing
   */
  private def loadTransformedData(dataSourcePath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(dataSourcePath)
  }

  /**
   * Gets data containing diagnosis in I10_DX1 column
   *
   * @param transformedDF transformed data after initial data processing
   * @param diagnosis ICD-10-CM code representing the diagnosis used for this model building
   * @return filtered data with single diagnosis and single procedure
   */
  private def getOneDiagOneProcData(transformedDF: DataFrame, diagnosis: String): DataFrame = {
    import spark.implicits._
    transformedDF.filter($"I10_DX2".isNull &&
                         $"I10_DX1".isNotNull &&
                         $"I10_PR2".isNull &&
                         $"I10_PR1".isNotNull &&
                         $"I10_DX1".contains(diagnosis))
  }

  val toDouble: UserDefinedFunction = udf((s: String, lowerLimit: Int) => {
    if (s != null && (s forall Character.isDigit) && s.toDouble >= lowerLimit) s.toDouble
    else -1
  })

  /**
   * Converts numeric columns from String type to Double type using toDouble UDF.
   * Non-digits are considered as invalid/missing values and will be assigned -1 to be handled in the next step.
   *
   * @param oneDiagnosisDF data with single diagnosis and single procedure
   * @return updated data with numeric columns converted to double type
   */
  private def convertNumericColumnsToDouble(oneDiagnosisDF: DataFrame): DataFrame = {
    var convertedDF = oneDiagnosisDF
    for (colName <- allNumericColumns) {
      convertedDF = convertedDF.withColumn(colName, toDouble(col(colName), lit(0)))
    }
    convertedDF
  }

  /**
   * Removes any invalid/missing values by filtering out values less than 0 from numeric columns
   *
   * @param convertedDF data with numeric columns converted to double type
   * @return updated data with invalid/missing values removed
   */
  private def removeInvalidData(convertedDF: DataFrame): DataFrame = {
    var filteredDF = convertedDF
    for (colName <- allNumericColumns) {
      filteredDF = filteredDF.filter(col(colName) >= 0)
    }
    filteredDF
  }
}
