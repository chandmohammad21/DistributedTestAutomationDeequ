package org.data2scoops.Validate

import com.google.gson.Gson
import org.apache.spark.sql.functions._
import org.data2scoops.Validate.helpers._

object DistributedDataValidation {

  /** Deserializes the inputObject into an instance of RunConfiguration. This state is necessary
   * for an assortment of things.  Refer to the RunConfiguration case class
   *  @param inputObject[String] serialized json object that is provided to the main class
   *  @return a RunConfiguration object
   */
  def parseInputObject(inputObject: String, Log: Logger): Option[RunConfiguration] = {
    val logError = (message: String, ex: Exception) => {
      Log.logging(this.getClass.getSimpleName, "ERROR",
        s"Exception: ${ex.getClass}. StackTrace: ${ex.getStackTrace
          .mkString("Array(", ", ", ")")}. Message: $message")
    }
    try {
      Some(new Gson().fromJson(inputObject, classOf[RunConfiguration]))
    }
    catch {
      case ex: java.lang.NullPointerException =>
        logError("Missing a key in input object.", ex)
        None
      case ex: com.google.gson.JsonSyntaxException =>
        logError("JSON is malformatted.", ex)
        None
    }
  }

  /** Entry method for Rate Table Verification execution
   *  @param args Parameters supplied to main method for Rate Table Verification run.
   *              These are a serialized json object that are provided in args[0]
   *  @return the status of a run (1: SUCCESS, 0: FAILURE)
   */
  def main(args: Array[String]): Unit = {
    val log = new Logger()
    val inputObject = parseInputObject(args(0), log).get
    val scopeName = sys.env("DATABRICKS_SECRET_SCOPE")
    val tenantId = sys.env("TENANT_ID")
    val appName = sys.env("AZURE_SUBSCRIPTION_NAME")
    val fileName = inputObject.fileName
    val runMode = inputObject.runMode

    //Initialize spark Session with client Authorization
    val spark = Gateway.sessionBuilder(s"${appName}_VarificationApp", inputObject
      .storageAccount,
      scopeName, tenantId, log, runMode)
    import spark.implicits._
    log.logging(this.getClass.getSimpleName, "INFO",
      s"Processing STARTED for File: ${inputObject.fileName}")
    val inputPath = FileFactory.getPath(inputObject, "INPUT")
    val outputPath = FileFactory.getPath(inputObject, "OUTPUT")
    log.logging(this.getClass.getSimpleName, "INFO" ,s"Input Path: $inputPath," +
      s" Output Path: $outputPath")

    // Check if a File definition exists for provided file type
    if (FileFactory.getFile(fileName).isDefined) {
      val data = Gateway.readObject(spark, inputObject, "INPUT", log).cache()
      log.logging(this.getClass.getSimpleName, "INFO", s"Verifying $fileName")
      if(!data.isEmpty) {
        val file = FileFactory.getFile(fileName).get

        // Perform Verification
        log.logging(this.getClass.getSimpleName, "INFO", s"Verification STARTED")
        val verificationRes = DataValidator.runVerification(data, file)
        log.logging(this.getClass.getSimpleName, "INFO", s"Verification COMPLETED")

        val stdRes = DataValidator.standardizeVerificationResults(verificationRes, file)
        val stdResStr = stdRes.map(r =>
          s"${r.FileName}\t${r.ColumnName}\t${r.TestCase}\t${r.Result}\n")
          .reduce(_ + _)
        log.logging(this.getClass.getSimpleName, "INFO", stdResStr)

        //Formatting csv data
        val stdResDf = stdRes.toDF()
          .withColumn("TestCase", regexp_replace(col("TestCase"), ",", "|"))
          .withColumn("TestCaseID", concat(col("FileName"),
            lit("_TC_"), monotonically_increasing_id()))
        val stdResDfOut = stdResDf
          .select("FileName", "ColumnName", "TestCaseID", "Result", "TestCase")

        Gateway.writeObjectCsv(inputObject, stdResDfOut, "OUTPUT", log)

      } else {
        log.logging(this.getClass.getSimpleName, "INFO", s"Verification SKIPPED - DataFrame Empty")
      }
      data.unpersist

    } else {
      log.logging(this.getClass.getSimpleName, "INFO", s"No File definition found for $fileName")
      throw new Exception(s"No File definition found for $fileName")
    }
  }
}