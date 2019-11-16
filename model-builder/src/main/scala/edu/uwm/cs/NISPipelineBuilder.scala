package edu.uwm.cs

import com.amazonaws.services.sagemaker.sparksdk.algorithms.XGBoostSageMakerEstimator
import com.amazonaws.services.sagemaker.sparksdk.{CustomNamePolicyFactory, EndpointCreationPolicy, IAMRole, S3DataPath}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}

class NISPipelineBuilder(numericColumns: Array[String],
                         stringIndexColumns: Array[String]) extends Serializable {

  def buildPipeline(sageMakerRoleArn: String,
                    sageMakerTrainingInstanceType: String,
                    sageMakerTrainingInstanceCount: Int,
                    sageMakerEndpointInstanceType: String,
                    sageMakerEndpointInitialInstanceCount: Int,
                    sageMakerBucketName: String,
                    sageMakerInputPrefix: String,
                    sageMakerOutputPrefix: String): Pipeline = {

    import scala.collection.mutable.ArrayBuffer
    var stages = ArrayBuffer[PipelineStage]()

    for(colName <- stringIndexColumns) {
      var indexer = new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName+"_IND")
      var encoder = new OneHotEncoderEstimator()
        .setInputCols(Array(indexer.getOutputCol))
        .setOutputCols(Array(colName+"_ENC"))
        .setHandleInvalid("keep")
      stages += indexer
      stages += encoder
    }

    val numericColumns_VA = Array("LOS")
    val stringIndexColumns_VA = Array("I10_PR1_ENC","HOSP_DIVISION_ENC")

    val assembler = new VectorAssembler()
      .setInputCols(numericColumns_VA ++ stringIndexColumns_VA)
      .setOutputCol("features")
    stages += assembler

    val customTransformer = new CustomTransformer("custometransfomer")
    stages += customTransformer

    val xgBoostSageMakerEstimator = new XGBoostSageMakerEstimator(
      sagemakerRole=IAMRole(sageMakerRoleArn),
      trainingInstanceType = sageMakerTrainingInstanceType,
      trainingInstanceCount = sageMakerTrainingInstanceCount,
      endpointInstanceType = sageMakerEndpointInstanceType,
      endpointInitialInstanceCount = sageMakerEndpointInitialInstanceCount,
      trainingInputS3DataPath = S3DataPath(sageMakerBucketName, sageMakerInputPrefix),
      trainingOutputS3DataPath = S3DataPath(sageMakerBucketName, sageMakerOutputPrefix),
      endpointCreationPolicy = EndpointCreationPolicy.CREATE_ON_CONSTRUCT,
      namePolicyFactory = new CustomNamePolicyFactory("trainingJobName","modelName","endpointConfigName","endpointName")
    )
    xgBoostSageMakerEstimator.setNumRound(15)
    xgBoostSageMakerEstimator.setObjective("reg:linear")
    stages += xgBoostSageMakerEstimator

    new Pipeline().setStages(stages.toArray)
  }
}
