package edu.uwm.cs

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SaveMode, SparkSession}

class NISDataProcessingService(dataSourcePath: String,
                               diagnosisIndexTablePath: String,
                               externalCauseIndexTablePath: String,
                               procedureIndexTablePath: String) extends Serializable {

  lazy val spark: SparkSession = SparkSession.builder
//                                             .master("local[*]") // remove when submitting to EMR
                                             .getOrCreate

  lazy val bc_diagnosisIndexMap: Broadcast[collection.Map[String, String]] = getDiagnosisIndexMap
  lazy val bc_externalCauseIndexMap: Broadcast[collection.Map[String, String]] = getExternalCauseIndexMap
  lazy val bc_procedureIndexMap: Broadcast[collection.Map[String, String]] = getProcedureMap

  val nisDataParser = new NISDataParser
  val nisDataConverter = new NISDataConverter(bc_diagnosisIndexMap, bc_externalCauseIndexMap, bc_procedureIndexMap)

  /**
   * Processes 2016 NIS Core Data
   * @return DataFrame with 99 data elements converted as double type
   */
  def process(): DataFrame = {
    val originalDF = loadData()
    val parsedDF = nisDataParser.parseData(originalDF)
    nisDataConverter.convertData(parsedDF)
  }

  /**
   * Saves transformed data to S3 in csv format
   * @param processedDF DataFrame with processed data
   * @param dataOutputPath S3 location to store output
   */
  def saveCsvToS3(processedDF: DataFrame, dataOutputPath: String): Unit = {
    processedDF.write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(dataOutputPath)
  }

  /**
   * Loads original data from given file path
   * @return DataFrame with a single column containing value
   */
  private def loadData(): DataFrame = {
    spark.read
      .format("text")
      .option("header", "false")
      .load(dataSourcePath)
  }

  /**
   * Converts the diagnosis index table to map and broadcasts the map
   * @return Broadcast of diagnosis index map <br> (key: ICD-10-CM diagnosis code, value: index)
   */
  def getDiagnosisIndexMap: Broadcast[collection.Map[String, String]] = {
    implicit val encoderString: Encoder[(String, String)] = Encoders.bean[(String,String)](classOf[(String, String)])
    val diagnosisIndexDF = spark.read.format("csv")
      .option("header", "false")
      .load(diagnosisIndexTablePath)
    val diagnosisIndexMap = diagnosisIndexDF.select("*")
      .map(r => (r.getString(1), r.getString(0)))
      .rdd.collectAsMap()
    spark.sparkContext.broadcast(diagnosisIndexMap)
  }

  /**
   * Converts the external cause index table to map and broadcasts the map
   * @return Broadcast of external cause index map <br> (key: ICD-10-CM external cause code, value: index)
   */
  def getExternalCauseIndexMap: Broadcast[collection.Map[String, String]] = {
    implicit val encoderString: Encoder[(String, String)] = Encoders.bean[(String,String)](classOf[(String, String)])
    val externalCauseIndexDF = spark.read.format("csv")
      .option("header", "false")
      .load(externalCauseIndexTablePath)
    val externalCauseIndexMap = externalCauseIndexDF.select("*")
      .map(r => (r.getString(1), r.getString(0)))
      .rdd.collectAsMap()
    spark.sparkContext.broadcast(externalCauseIndexMap)
  }

  /**
   * Converts the procedure index table to map and broadcasts the map
   * @return Broadcast of procedure index map <br> (key: ICD-10-PCS procedure code, value: index)
   */
  def getProcedureMap: Broadcast[collection.Map[String, String]] = {
    implicit val encoderString: Encoder[(String, String)] = Encoders.bean[(String,String)](classOf[(String, String)])
    val procedureIndexDF = spark.read.format("csv")
      .option("header", "false")
      .load(procedureIndexTablePath)
    val procedureIndexMap = procedureIndexDF.select("*")
      .map(r => (r.getString(1), r.getString(0)))
      .rdd.collectAsMap()
    spark.sparkContext.broadcast(procedureIndexMap)
  }
}
