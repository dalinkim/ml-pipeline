package edu.uwm.cs

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, col}

class NISDataConverter(bc_diagnosisIndexMap: Broadcast[collection.Map[String, String]],
                       bc_externalCauseIndexMap: Broadcast[collection.Map[String, String]],
                       bc_procedureIndexMap: Broadcast[collection.Map[String, String]]) extends Serializable {

  val diagnosisIndexLookupUDF: UserDefinedFunction = udf((diagnosis_code: String) => {
    if (diagnosis_code == "") "0" else bc_diagnosisIndexMap.value(diagnosis_code)
  })
  val externalCauseIndexLookupUDF: UserDefinedFunction = udf((external_cause_code: String) => {
    if (external_cause_code == "") "0" else bc_externalCauseIndexMap.value(external_cause_code)
  })
  val procedureIndexLookupUDF: UserDefinedFunction = udf((procedure_code: String) => {
    if (procedure_code == "") "0" else bc_procedureIndexMap.value(procedure_code)
  })

  /**
   * Replaces ICD-10-CM/PCS codes with index numbers and converts all column types to double
   * @param parsedDF DataFrame to update
   * @return DataFrame with double type columns
   */
  def convertData(parsedDF: DataFrame): DataFrame = {
    val codedDF = replaceICDCodesWithIndex(parsedDF)
    val convertedDF = convertToDouble(codedDF)
    convertedDF
  }

  /**
   * Converts data type of all columns to double
   * @param codedDF DataFrame to update (all columns should be convertible to double)
   * @return DataFrame with double type columns
   */
  private def convertToDouble(codedDF: DataFrame): DataFrame = {
    codedDF.columns.foldLeft(codedDF) {
      (tempDF, colName) => tempDF.withColumn(colName, col(colName).cast("double"))
    }
  }

  /**
   * Replaces ICD-10-CM/PCS codes with index numbers
   * @param df DataFrame to update
   * @return updated DataFrame
   */
  private def replaceICDCodesWithIndex(df: DataFrame): DataFrame = {
    df.
      withColumn("I10_DX1", diagnosisIndexLookupUDF(col("I10_DX1"))).
      withColumn("I10_DX2", diagnosisIndexLookupUDF(col("I10_DX2"))).
      withColumn("I10_DX3", diagnosisIndexLookupUDF(col("I10_DX3"))).
      withColumn("I10_DX4", diagnosisIndexLookupUDF(col("I10_DX4"))).
      withColumn("I10_DX5", diagnosisIndexLookupUDF(col("I10_DX5"))).
      withColumn("I10_DX6", diagnosisIndexLookupUDF(col("I10_DX6"))).
      withColumn("I10_DX7", diagnosisIndexLookupUDF(col("I10_DX7"))).
      withColumn("I10_DX8", diagnosisIndexLookupUDF(col("I10_DX8"))).
      withColumn("I10_DX9", diagnosisIndexLookupUDF(col("I10_DX9"))).
      withColumn("I10_DX10", diagnosisIndexLookupUDF(col("I10_DX10"))).
      withColumn("I10_DX11", diagnosisIndexLookupUDF(col("I10_DX11"))).
      withColumn("I10_DX12", diagnosisIndexLookupUDF(col("I10_DX12"))).
      withColumn("I10_DX13", diagnosisIndexLookupUDF(col("I10_DX13"))).
      withColumn("I10_DX14", diagnosisIndexLookupUDF(col("I10_DX14"))).
      withColumn("I10_DX15", diagnosisIndexLookupUDF(col("I10_DX15"))).
      withColumn("I10_DX16", diagnosisIndexLookupUDF(col("I10_DX16"))).
      withColumn("I10_DX17", diagnosisIndexLookupUDF(col("I10_DX17"))).
      withColumn("I10_DX18", diagnosisIndexLookupUDF(col("I10_DX18"))).
      withColumn("I10_DX19", diagnosisIndexLookupUDF(col("I10_DX19"))).
      withColumn("I10_DX20", diagnosisIndexLookupUDF(col("I10_DX20"))).
      withColumn("I10_DX21", diagnosisIndexLookupUDF(col("I10_DX21"))).
      withColumn("I10_DX22", diagnosisIndexLookupUDF(col("I10_DX22"))).
      withColumn("I10_DX23", diagnosisIndexLookupUDF(col("I10_DX23"))).
      withColumn("I10_DX24", diagnosisIndexLookupUDF(col("I10_DX24"))).
      withColumn("I10_DX25", diagnosisIndexLookupUDF(col("I10_DX25"))).
      withColumn("I10_DX26", diagnosisIndexLookupUDF(col("I10_DX26"))).
      withColumn("I10_DX27", diagnosisIndexLookupUDF(col("I10_DX27"))).
      withColumn("I10_DX28", diagnosisIndexLookupUDF(col("I10_DX28"))).
      withColumn("I10_DX29", diagnosisIndexLookupUDF(col("I10_DX29"))).
      withColumn("I10_DX30", diagnosisIndexLookupUDF(col("I10_DX30"))).
      withColumn("I10_ECAUSE1", externalCauseIndexLookupUDF(col("I10_ECAUSE1"))).
      withColumn("I10_ECAUSE2", externalCauseIndexLookupUDF(col("I10_ECAUSE2"))).
      withColumn("I10_ECAUSE3", externalCauseIndexLookupUDF(col("I10_ECAUSE3"))).
      withColumn("I10_ECAUSE4", externalCauseIndexLookupUDF(col("I10_ECAUSE4"))).
      withColumn("I10_PR1", procedureIndexLookupUDF(col("I10_PR1"))).
      withColumn("I10_PR2", procedureIndexLookupUDF(col("I10_PR2"))).
      withColumn("I10_PR3", procedureIndexLookupUDF(col("I10_PR3"))).
      withColumn("I10_PR4", procedureIndexLookupUDF(col("I10_PR4"))).
      withColumn("I10_PR5", procedureIndexLookupUDF(col("I10_PR5"))).
      withColumn("I10_PR6", procedureIndexLookupUDF(col("I10_PR6"))).
      withColumn("I10_PR7", procedureIndexLookupUDF(col("I10_PR7"))).
      withColumn("I10_PR8", procedureIndexLookupUDF(col("I10_PR8"))).
      withColumn("I10_PR9", procedureIndexLookupUDF(col("I10_PR9"))).
      withColumn("I10_PR10", procedureIndexLookupUDF(col("I10_PR10"))).
      withColumn("I10_PR11", procedureIndexLookupUDF(col("I10_PR11"))).
      withColumn("I10_PR12", procedureIndexLookupUDF(col("I10_PR12"))).
      withColumn("I10_PR13", procedureIndexLookupUDF(col("I10_PR13"))).
      withColumn("I10_PR14", procedureIndexLookupUDF(col("I10_PR14"))).
      withColumn("I10_PR15", procedureIndexLookupUDF(col("I10_PR15")))
  }
}
