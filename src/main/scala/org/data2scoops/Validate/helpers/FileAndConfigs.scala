package org.data2scoops.Validate.helpers

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, round, trim}
import org.apache.spark.sql.types.StructType

case class RunConfiguration(
     storageAccount: String,
     storageAccountContainer: String,
     fileName: String,
     runMode: String
)

case class TestResultFormat(
     FileName: String,
     ColumnName: String,
     TestCaseID: String,
     Result: String,
     TestCase: String
)

object FileFactory {
  def getFile(name: String): Option[FileInfo] = name match {
    case x if x contains "DISTRIBUTOR" => Some(new DISTRIBUTOR(name))
    case _ => throw new Exception("Unknown File Type")
  }

  def getPath(inputObject: RunConfiguration, fType: String): String = {
    val root = inputObject.runMode.toUpperCase match {
        case "LOCAL" => "C:/tmp/data/datafiles/"
        case "CLUSTER" => s"abfss://${inputObject.storageAccountContainer}" +
          s"@${inputObject.storageAccount}.dfs.core.windows.net/datafiles/"
    }
    val baseFolder = inputObject.fileName.toUpperCase match {
      case "DISTRIBUTOR" => "DISTRIBUTOR_FILES"
    }
    fType match {
      case "INPUT" => root +
        s"$baseFolder/${inputObject.fileName.toUpperCase}"
      case "OUTPUT" => root +
        s"$baseFolder/${inputObject.fileName.toUpperCase}_VALIDATION"
    }
  }
}

object PossibleValues {
  val genderArray: Array[String] = Array(
    "M","F")
  val ageArray: Array[String] = (0 to 100).map(_.toString()).toArray
  val distributorArray: Array[String] = Array(
    "DIST-P59d", "DIST-O41h", "DIST-P56b", "DIST-P43",
    "DIST-P37j", "DIST-P89", "DIST-P31e", "DIST-P40a",
    "DIST-P37d", "DIST-P67", "DIST-O41f", "DIST-O18",
    "DIST-O16b", "DIST-P66", "DIST-P21a", "DIST-I23c",
    "DIST-O41e", "DIST-P21c", "DIST-P21b", "DIST-P83",
    "DIST-O32b", "DIST-O41b", "DIST-P21d", "DIST-P15",
    "DIST-I11b", "DIST-P37b", "DIST-P16", "DIST-I23b",
    "DIST-O10b", "DIST-I11a", "DIST-O17", "DIST-P81e",
    "DIST-P58c", "DIST-I14a", "DIST-P37g", "DIST-O32a",
    "DIST-P55b", "DIST-P55c", "DIST-P37h", "DIST-P40c",
    "DIST-I24", "DIST-P56a", "DIST-P63b", "DIST-P37i",
    "DIST-P58d", "DIST-P63c", "DIST-P57c", "DIST-O51d",
    "DIST-O12b", "DIST-O13b", "DIST-I13a", "DIST-P32d",
    "DIST-P21e", "DIST-P84", "DIST-P45", "DIST-P82b",
    "DIST-O41d", "DIST-P41", "DIST-P44", "DIST-O15",
    "DIST-P59a", "DIST-O51b", "DIST-P54", "DIST-I13b",
    "DIST-P14", "DIST-P61a", "DIST-O41j", "DIST-P42",
    "DIST-O10a", "DIST-I14b", "DIST-P65", "DIST-O51c",
    "DIST-P37f", "DIST-P81g", "DIST-P58f", "DIST-P55f",
    "DIST-P32c", "DIST-O31b", "DIST-P51b", "DIST-P81c",
    "DIST-I31", "DIST-P34a", "DIST-O14a", "DIST-O11",
    "DIST-P59c", "DIST-P33", "DIST-I22a", "DIST-P85",
    "DIST-P81b", "DIST-P11", "DIST-P37c", "DIST-O16a",
    "DIST-O31a", "DIST-P59b", "DIST-I22b", "DIST-P59f",
    "DIST-O14b", "DIST-P36", "DIST-P31f", "DIST-I21a",
    "DIST-I23a", "DIST-O41a", "DIST-P40d", "DIST-P37a",
    "DIST-P31d", "DIST-P53", "DIST-P82a", "DIST-P58e",
    "DIST-P40b", "DIST-I21b", "DIST-P81a", "DIST-I25",
    "DIST-P37e", "DIST-O41l", "DIST-P37k", "DIST-O51a",
    "DIST-P55d", "DIST-P55e", "DIST-O14c", "DIST-P35",
    "DIST-O13a", "DIST-O12a", "DIST-P51a", "DIST-P34b",
    "DIST-P59e", "DIST-P61b", "DIST-P57b", "DIST-O41g",
    "DIST-P57a", "DIST-P63a", "DIST-I12", "DIST-P13"
  )
  val nullValueArray: Array[String] = Array()
  val allStatesArray: Array[String] = Array(
    "PARIS", "INDIA", "OKLAHOMA")
}

case class VerificationOutput(
    FileName: String,
    ColumnName: String,
    TestCase: String,
    Result: String
)

trait FileInfo {
  val fileName: String
  protected val checkName: String = s"$fileName"

  val groupingColumns: Seq[String]
  val numericColumns: Seq[String]

  def getChecks(): Seq[Check]

  def customChecks: Boolean

  protected def range(column: String, from: Integer, to: Integer) =
    s"$column BETWEEN $from AND $to"

  protected def rangeDouble(column: String, from: Double, to: Double) =
    s"$column BETWEEN $from AND $to"

  protected def blank(column: String) =
    s"ISNULL(NULLIF($column, ''))"

  protected def upper(column: String) =
    s"COALESCE($column, '') == COALESCE(UPPER($column), '')"

  protected def trimming(column: String) =
    s"COALESCE($column, '') == COALESCE(TRIM($column), '')"

  protected def greaterThanZero(column: String) =
    s"COALESCE($column, 1) > 0 "

  protected def isColumnsNull(cols: Seq[String]): String = {
    cols
      .map(col(_).isNull)
      .reduce(_ and _)
      .toString
  }

  protected def isColumnNull(col: String): String = {
    s"$col is Null"
  }

  protected def equal(column: String, value: Double) =
    s"$column == $value"
}

class DISTRIBUTOR(val fileName: String)
  extends FileInfo {

  override val groupingColumns: Seq[String] = Seq()
  override val numericColumns: Seq[String] = Seq()

  override def getChecks(): Seq[Check] = {
    Seq(Check(CheckLevel.Error, checkName)
      .isContainedIn("DISTIBUTOR_ID", {PossibleValues.distributorArray})
      .isComplete("DISTIBUTOR_ID")
      .isContainedIn("STATE", {PossibleValues.allStatesArray})
      .isComplete("STATE")
      .satisfies(upper("STATE"), "STATE should be UpperCase")
      .satisfies(trimming("STATE"), "STATE should be Trimmed")
      .isContainedIn("GENDER", {PossibleValues.genderArray})
      .isComplete("GENDER")
      .satisfies(upper("GENDER"), "STATE should be UpperCase")
      .satisfies(trimming("GENDER"), "STATE should be Trimmed")
      .satisfies(range("AGE", 0, 100), "AGE should be between 0 and 100")
      .isNonNegative("AGE")
      .isComplete("AGE")
      .satisfies(trimming("AGE"), "AGE should be Trimmed")
      .isComplete("SALARY")
      .isNonNegative("SALARY")
      .satisfies(equal("SALARY", 0), "SALARY should be zero when age < 15")
      .where("AGE < 15")
    )
  }

  override def customChecks: Boolean = false
}

object DataValidator {
  def runVerification(data: DataFrame, file: FileInfo)
  : VerificationResult = {
    VerificationSuite()
      .onData(data)
      .addChecks(file.getChecks())
      .run()
  }

  def standardizeVerificationResults(verificationResult: VerificationResult, file: FileInfo)
  : Seq[VerificationOutput] = {
    verificationResult.checkResults.values.head.constraintResults.map { result => {
      val tmp = result.metric.get.instance.indexOf(" ")
      val colName = if (tmp > 0) {
        result.metric.get.instance.substring(0,tmp)
      } else {
        result.metric.get.instance
      }
      val testcase = result.constraint.toString
        .substring(result.constraint.toString
          .indexOf("(") + 1).substring(0, result.constraint.toString
        .substring(result.constraint.toString
          .indexOf("(") + 1).length -1)

      VerificationOutput(file.fileName, colName, testcase, result.status.toString)
    }}
  }

  def applyFnToColumns(df: DataFrame, colList: Seq[String], fn: Column => Column)
  : DataFrame = {
    colList.foldLeft(df)((acc, x) => acc.withColumn(x, fn(col(x))))
  }

  def cleanStr(col:Column): Column = trim(col)

  def roundNum(col:Column): Column = round(col, 0)

  def cleanStrCols(df: DataFrame, colList: Seq[String]): DataFrame = {
    applyFnToColumns(df, colList, cleanStr)
  }

  def cleanRoundCols(df: DataFrame, colList: Seq[String]): DataFrame = {
    applyFnToColumns(df, colList, roundNum)
  }

  def getSelectedTable(
      df: DataFrame,
      groupingColumns: Seq[String],
      numericColumns: Seq[String])
    : DataFrame = {
    val selectedTable = df.select((groupingColumns ++ numericColumns).head,
      (groupingColumns ++ numericColumns).tail:_*)
    cleanStrCols(selectedTable, groupingColumns)
  }

  def getRoundedTable(
      df: DataFrame,
      numericColumns: Seq[String])
    : DataFrame = {
    cleanRoundCols(df, numericColumns)
  }

  def isColumnEqual(
      df1: DataFrame,
      df2:DataFrame,
      groupingColumns: Seq[String],
      column: String)
    : Boolean = {
    val finalDf1 = df1
      .select((groupingColumns :+ column).head, (groupingColumns :+ column).tail:_*)

    val finalDf2 = df2
      .select((groupingColumns :+ column).head, (groupingColumns :+ column).tail:_*)

    finalDf1.except(finalDf2).union(finalDf2.except(finalDf1)).isEmpty
  }

  def boolToStatus(bool: Boolean): String = {
    if (bool) {
      "Success"
    } else {
      "Failure"
    }
  }

  def getEmptyDataFrame(spark: SparkSession): DataFrame = {
    val schema = ScalaReflection.schemaFor[TestResultFormat].dataType.asInstanceOf[StructType]
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }
}
