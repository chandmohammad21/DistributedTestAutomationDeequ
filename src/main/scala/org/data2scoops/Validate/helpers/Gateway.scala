package org.data2scoops.Validate.helpers

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.{DataFrame, SparkSession}

object Gateway {

  /** This returns spark session as gateway to spark cluster
   * @param appName App name given to the spark app
   * @param azureStorageAccountName Azure storage account
   * @param scopeName Secret scope name of Azure storage account
   * @param tenantId tenant id of Azure storage account
   * @return SparkSession with client Azure authentication
   */
  def sessionBuilder (
      appName: String,
      azureStorageAccountName: String,
      scopeName: String,
      tenantId: String,
      Log: Logger,
      runMode: String)
    : SparkSession = {
    try {
      runMode.toLowerCase match {
        case "local" => SparkSession.builder()
          .appName(appName)
          .master("local")
          .getOrCreate()
        case "cluster" => SparkSession.builder()
          .appName(appName)
          .config(s"fs.azure.account.auth.type" +
            s".$azureStorageAccountName.dfs.core.windows.net",
            "OAuth")
          .config(s"fs.azure.account.oauth.provider.type" +
            s".$azureStorageAccountName.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
          .config(s"fs.azure.account.oauth2.client.id" +
            s".$azureStorageAccountName.dfs.core.windows.net",
            dbutils.secrets.get(scopeName, "clientId"))
          .config(s"fs.azure.account.oauth2.client.secret" +
            s".$azureStorageAccountName.dfs.core.windows.net",
            dbutils.secrets.get(scopeName, "clientKey"))
          .config(s"fs.azure.account.oauth2.client.endpoint" +
            s".$azureStorageAccountName.dfs.core.windows.net",
            s"https://login.microsoftonline.com/$tenantId/oauth2/token")
          .getOrCreate()
      }
    } catch {
      case ex: Exception =>
        Log.logging(
          this.getClass.getSimpleName,
          "ERROR",
          s"Failed while creating spark Session: ${ex.getMessage}.")
        throw ex
    }
  }

  /** Creates an object (CSV) in a specified blob container
   *
   * @param dataFrame  object to be written
   * @param fileType type of the object to be written
   * @param inputObject runConfig type of object
   * @return an option containing the DataFrame of the requested file
   */
  def writeObjectCsv(inputObject: RunConfiguration,
      dataFrame: DataFrame,
      fileType: String,
      Log: Logger)
    : Boolean = {
    val writeOptions: Map[String, String] =
      if (fileType contains "SUMMARY") {
        Map(
          ("delimiter", "\t"),
          ("header", "false")
        )
      } else {
        Map(
          ("header", "true")
        )
      }
    try {
      dataFrame.toDF().coalesce(1)
        .write
        .mode("overwrite")
        .options(writeOptions)
        .format("csv")
        .save(FileFactory.getPath(inputObject, fileType))
      true
    } catch {
      case ex: Exception =>
        Log.logging(
          this.getClass.getSimpleName,
          "ERROR",
          s"Failed to create the following object: $fileType. Exception: ${ex.getMessage}.")
        throw ex
    }
  }

  /** Reads an object (Parquet) in a specified blob container
   *
   * @param spark  SparkSession
   * @param fileType name of the object to be read
   * @param inputObject RunConfiguration object
   * @param log logger
   * @return DataFrame
   */
  def readObject(
      spark: SparkSession,
      inputObject: RunConfiguration,
      fileType: String,
      log: Logger)
    : DataFrame = {
    try {
      spark.read.option("header", "true")
        .format("csv").load(FileFactory.getPath(inputObject, fileType))
    } catch {
      case ex: Exception =>
        log.logging(
          this.getClass.getSimpleName,
          "ERROR",
          s"Failed to read the following object: $fileType. Exception: ${ex.getMessage}.")
        throw ex
    }
  }
}

