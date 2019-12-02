package edu.uwm.cs

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

/**
 * This custom transformer exists to transform a DataFrame to only have label and features columns
 * to resolve libsvm exception thrown by XGBoostSageMakerEstimator when input DataFrame contains more than these columns.
 *
 * See <a href="https://github.com/aws/sagemaker-spark/issues/47">https://github.com/aws/sagemaker-spark/issues/47</a>
 */
class SageMakerTransformer(override val uid: String) extends Transformer {
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final def getInputCol: String = $(inputCol)
  final def setInputCol(value: String): SageMakerTransformer = set(inputCol, value)

  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  final def getOutputCol: String = $(outputCol)
  final def setOutputCol(value: String): SageMakerTransformer = set(outputCol, value)

  def copy(extra: ParamMap): SageMakerTransformer = { defaultCopy(extra) }

  override def transformSchema(schema: StructType): StructType = { schema }

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.select("label","features")
  }
}