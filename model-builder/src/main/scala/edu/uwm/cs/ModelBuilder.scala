package edu.uwm.cs

import com.amazonaws.services.sagemaker.sparksdk.{IAMRole, S3DataPath, SageMakerModel}
import com.amazonaws.services.sagemaker.sparksdk.algorithms.XGBoostSageMakerEstimator
import com.amazonaws.services.sagemaker.sparksdk.algorithms.{LinearLearnerRegressor, LinearLearnerSageMakerEstimator}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ModelBuilder {
  val s3prefix = "s3://"
  val s3BucketName = "dalin-ml-pipeline"
  val dataSourcePath = "/transformed-csv/*.csv"

  val sageMakerInputPrefix = "sagemaker/trainingInput"
  val sageMakerOutputPrefix = "sagemaker/trainingOutput/LinearLearner"
  val sageMakerRoleArn = "arn:aws:iam::263690384742:role/SparkSageMakerRole"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate

    val processedDF = spark.read.format("csv")
      .option("header", "true")
      .load(s"$s3prefix$s3BucketName$dataSourcePath")

    val transformedRDD: RDD[LabeledPoint] = processedDF.rdd.map(row => {
      val fullArray = row.toSeq.toArray.map(_.asInstanceOf[Double])
      val featureArray = fullArray.slice(1, fullArray.length)
      LabeledPoint(fullArray(0), Vectors.dense(featureArray))
    })

    import spark.implicits._
    val transformedDF = transformedRDD.toDF("label", "features")

    val randomSplitDSs: Array[Dataset[Row]] = transformedDF.randomSplit(Array(0.7, 0.3), 11)
    val trainingDS: Dataset[Row] = randomSplitDSs(0)
    val testingDS: Dataset[Row] = randomSplitDSs(1)

    val linearLearnerRegressor = new LinearLearnerRegressor(
      sagemakerRole=IAMRole(sageMakerRoleArn),
      trainingInstanceType = "ml.m4.xlarge",
      trainingInstanceCount = 1,
      endpointInstanceType = "ml.m4.xlarge",
      endpointInitialInstanceCount=1,
      trainingInputS3DataPath = S3DataPath(s3BucketName, sageMakerInputPrefix),
      trainingOutputS3DataPath = S3DataPath(s3BucketName, sageMakerOutputPrefix)
    ).setFeatureDim(97)

    // Build the model
    val model: SageMakerModel = linearLearnerRegressor.fit(trainingDS)

    // Get predictions
    val predictions: DataFrame = model.transform(testingDS)

    // Collect predictions and labels
    val predictionAndLabels: RDD[(Double, Double)] = predictions.select($"prediction",$"score").as[(Double, Double)].rdd

    // Instantiate metrics object
    val metrics = new RegressionMetrics(predictionAndLabels)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")

    spark.close()
  }
}
