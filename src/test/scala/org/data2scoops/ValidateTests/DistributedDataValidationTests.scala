package org.data2scoops.ValidateTests

import java.io.File

import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data2scoops.Validate.DistributedDataValidation.parseInputObject
import org.data2scoops.Validate.helpers._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DistributedDataValidationTests
  extends FunSuite with DataFrameSuiteBase with DatasetSuiteBase with BeforeAndAfterAll {
  val log = new Logger()

  override lazy val spark: SparkSession = SparkSession.builder()
    .appName("UnitTests")
    .master("local").getOrCreate()
  import spark.implicits._

  def dataLocation(funcName: String): String = {
    s"""${System.getProperty("user.dir")}${File.separator}
       |testingData${File.separator}
       |rateTableValidationTest${File.separator}
       |func_$funcName""".stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString
  }

  def reader(format: String, header: Boolean, fileLoc: String)
  : DataFrame = {
    format.toLowerCase match {
      case "text" | "csv" =>  spark.read.format(format).option("header", header).load(fileLoc)
      case "parquet" | "json" => spark.read.format(format).load(fileLoc)
    }
  }

  def fileLoc(funcName:String, location: String, fileName: String): String = {
    s"${dataLocation(funcName)}${File.separator}$location${File.separator}$fileName"
  }

  test("Test 01 - Function parseInputObject"){
    val answer = reader("text", header = false,
      fileLoc("parseInputObject", "answers", "answer.txt"))
    val input = scala.io.Source
      .fromFile(fileLoc("parseInputObject", "input","inputFile.json"))
    val expected = answer.map(r => r.getString(0)).collect().mkString
    val actualResult =
      try
        parseInputObject(input.getLines.mkString, log).get.toString
      finally input.close()
    assert(expected, actualResult)
  }

  test("Test 02 - Function getPath Input"){
    val answer = reader("text", header = false, fileLoc("getPath_input", "answers", "answer.txt"))
    val input = reader("text", header = false, fileLoc("getPath_input", "input", "inputFile.txt"))
    val expected = answer.map(r => r.getString(0)).collect().mkString
    val inputOne = input.map(r => r.getString(0)).collect().mkString
    val inputObject = RunConfiguration(inputOne.split(",")(0),
      inputOne.split(",")(1),
      inputOne.split(",")(2),
      inputOne.split(",")(3))
    val actualResult = FileFactory.getPath(inputObject, "INPUT")
    assert(expected, actualResult)
  }

  test("Test 03 - Function getPath Output"){
    val answer = reader("text", header = false, fileLoc("getPath_output", "answers", "answer.txt"))
    val input = reader("text", header = false, fileLoc("getPath_output", "input", "inputFile.txt"))
    val expected = answer.map(r => r.getString(0)).collect().mkString
    val inputOne = input.map(r => r.getString(0)).collect().mkString
    val inputObject = RunConfiguration(inputOne.split(",")(0),
      inputOne.split(",")(1),
      inputOne.split(",")(2),
      inputOne.split(",")(3))
    val actualResult = FileFactory.getPath(inputObject, "OUTPUT")
    assert(expected, actualResult)
  }

  test("Test 04 - Function getFile") {
    val input = reader("text", header = false, fileLoc("getFile", "input", "inputFile.txt"))
      .map(r => r.getString(0)).collect().mkString
    val actualResult = FileFactory.getFile(input).get.getClass
    val expected = new DISTRIBUTOR(input).getClass
    assert(expected, actualResult)
  }

  test("Test 05 - Function standardizeVerificationResults") {
    val input = reader("csv", header = true, fileLoc("standardizeVerificationResults", "input",
      "inputFile"))
    val file = FileFactory.getFile("DISTRIBUTOR").get
    val result = DataValidator.runVerification(input, file)
    val actualResult = DataValidator.standardizeVerificationResults(result, file).toDF
    val expected = reader("csv", header = true,
      fileLoc("standardizeVerificationResults", "answers",
        "answer"))
    assertDataFrameEquals(actualResult, expected)
  }

  test("Test 06 - Function getEmptyDataFrame") {
    val input = DataValidator.getEmptyDataFrame(spark)
    val expected = reader("csv", header = true, fileLoc("getEmptyDataFrame",
      "answers", "answer.txt"))
    assertDataFrameEquals(input, expected)
  }

  test("Test 07 - Function runVerification for DISTRIBUTOR") {
    val input = reader("csv", header = true, fileLoc("runVerification_DISTRIBUTOR",
      "input", "DISTRIBUTOR"))
    val file = FileFactory.getFile("DISTRIBUTOR").get
    val result = DataValidator.runVerification(input, file)
    val actualResult = checkResultsAsDataFrame(spark, result)
      .select("check", "constraint", "constraint_status")
    val expected = reader("csv", header = true, fileLoc("runVerification_DISTRIBUTOR",
      "answers", "DISTRIBUTOR"))
    assertDataFrameEquals(actualResult, expected)
  }


  // Stop spark session, this snippet is optional
  override def afterAll(): Unit = {
    spark.stop()
  }
}
