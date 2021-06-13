package org.data2scoops.Validate.helpers

import java.util
import java.util.Calendar
import com.microsoft.applicationinsights.{TelemetryClient, TelemetryConfiguration}
import scala.collection.JavaConversions._

abstract class Log extends Serializable {
  def logEvent(properties: Map[String, String] = Map()): Unit
  def setLoggerState(path: Seq[String]): Unit
  def logging(procedure: String, recordType: String, message: String): Unit
}

class Logger extends Log {
  /** Method to push data to appinsight
   */
  var workFlowID : String = "0"
  var clientName : String = "DEMO"
  var startTime : Calendar = Calendar.getInstance()
  var mostRecentEventTime : Calendar = Calendar.getInstance()

  override def logEvent(properties: Map[String, String]=Map()): Unit = {
    try {

      val configuration = TelemetryConfiguration.createDefault()
      val instrumentationKey = sys.env.getOrElse(
        "AZURE_INSTRUMENTATION_KEY_SB",
        "f2662bbc-e40a-4520-b78a-c76a12461c9a")
      configuration.setInstrumentationKey(instrumentationKey)
      val telemetryClient = new TelemetryClient(configuration)

      System.err.println(
        s"Event $workFlowID|Properties ${properties.map(kv => s"${kv._1}=${kv._2}").mkString(",")}")

      val appInsightsProperties = new util.HashMap[String, String](properties.size)
      properties.foreach(kv => appInsightsProperties.put(kv._1, kv._2))

      val appInsightsMetrics :Map[String, java.lang.Double] = Map("NULL" -> null)

      telemetryClient.trackEvent(workFlowID, appInsightsProperties, appInsightsMetrics)
      telemetryClient.flush()
    } catch {
      case exception: Exception =>
        System.err.println(s"Unable to send event $workFlowID to AppInsights")
        exception.printStackTrace(System.err)
    }
  }

  override def setLoggerState(path: Seq[String]): Unit = {
    workFlowID = path.reduce((acc,x) => s"$acc/$x")
    clientName = Option(sys.env.getOrElse("AZURE_SUBSCRIPTION_NAME", "DEFAULT"))
      .filterNot(_.isEmpty).getOrElse("DF")
  }

  override def logging(procedure : String , recordType : String , message: String) :Unit =  {

    val now = Calendar.getInstance()
    val timeElapsedSinceRunStart = (now.getTimeInMillis - startTime.getTimeInMillis).toString
    val timeSinceMostRecentCheckpoint =
      (now.getTimeInMillis - mostRecentEventTime.getTimeInMillis).toString
    mostRecentEventTime = Calendar.getInstance()

    val properties : Map[String, String] =
      Map("WorkflowID"-> workFlowID,
        "ClientName"-> clientName,
        "CallingProcedure"-> procedure,
        "RecordType"-> recordType,
        "Message" -> message,
        "DateTime" -> Calendar.getInstance().getTime.toString,
        "TimeElapsedSinceRunStart" -> timeElapsedSinceRunStart,
        "TimeElapsedSinceMostRecentCheckpoint" -> timeSinceMostRecentCheckpoint
      )
    //Logging into AppInsight
    logEvent(properties: Map[String, String])
  }
}

