package edu.uwm.cs

import org.apache.spark.sql.DataFrame

object ModelBuilder {

  final val sageMakerInputPrefix: String = "sagemaker/trainingInput"
  final val sageMakerOutputPrefix: String = "sagemaker/trainingOutput/XGBoost"

  final val allNumericColumns = Array("TOTCHG","AGE","AGE_NEONATE","AMONTH","AWEEKEND","DIED","DQTR","ELECTIVE","FEMALE","HCUP_ED","I10_NDX","I10_NECAUSE","I10_NPR","LOS")
  final val numericFeatureColumns: Array[String] = Array("LOS")
  final val stringIndexFeatureColumns: Array[String] = Array("I10_PR1","HOSP_DIVISION")

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      throw new IllegalArgumentException("Missing one or more required arguments.")
    }

    val dataSourceFilePath: String = args(0) // s3://dalin-ml-pipeline/transformed-csv/*.csv
    val s3BucketName: String = args(1) // my-ml-pipeline
    val sageMakerRoleArn: String = args(2) // arn:aws:iam::263690384742:role/SparkSageMakerRole
    val diagnosis: String = args(3)

    // extract data for diagnosis, do simple conversion, and remove invalid/missing data
    val nisModelBuildingService = new NISModelBuildingService(allNumericColumns)
    val preparedDF: DataFrame = nisModelBuildingService.prepareData(dataSourceFilePath, diagnosis)

    // Spark pipeline contains a chain of transformers and SageMaker estimator
    val nisPipelineBuilder = new NISPipelineBuilder(numericFeatureColumns, stringIndexFeatureColumns)
    val pipeline = nisPipelineBuilder.buildPipeline(s3BucketName,
                                                    sageMakerRoleArn,
                                                    sageMakerInputPrefix,
                                                    sageMakerOutputPrefix)

    // build and deploy the model
    nisModelBuildingService.buildAndDeployModel(preparedDF, pipeline)
  }
}
