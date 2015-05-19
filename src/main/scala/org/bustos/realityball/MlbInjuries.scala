package org.bustos.realityball

import org.scalatest._
import selenium._
import org.joda.time._
import org.joda.time.format._
import org.scalatest.time.{ Span, Seconds }
import org.openqa.selenium.support.ui.{ WebDriverWait, ExpectedCondition }
import org.openqa.selenium._
import org.openqa.selenium.By._
import htmlunit._
import remote._
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballData

class MlbInjuries extends Chrome {

  implicitlyWait(Span(20, Seconds))

  val logger = LoggerFactory.getLogger(getClass)
  logger.info("********************************")
  logger.info("*** Retrieving injury report ***")
  logger.info("********************************")

  val realityballData = new RealityballData
  val mlbIdExpression: Regex = "(.*)=(.*)".r

  val host = MlbURL
  go to host + "mlb/fantasy/injuries"
  Thread sleep 5

  val timeOfReport: Long = System.currentTimeMillis / 1000

  val reportTimeString: String = {
    val currentTime = new DateTime
    val formatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss z")
    formatter.print(currentTime)
  }

  def mlbIdFromMlbUrl(mlbUrl: WebElement): String = {
    val url = mlbUrl.findElement(new ByTagName("a")).getAttribute("href")
    url match {
      case mlbIdExpression(urlString, mlbId) =>
        realityballData.playerFromMlbId(mlbId, ""); mlbId
      case _ => throw new IllegalStateException("No player found")
    }
  }

  val injuries = (find("sub") match {
    case Some(x) => x.underlying match {
      case remoteElement: RemoteWebElement => remoteElement.findElementsByClassName("injuryItem") match {
        case reports: java.util.List[WebElement] => {
          reports.toList.map { report =>
            report match {
              case remoteReport: RemoteWebElement => {
                val reportColumns = remoteReport.findElementsByTagName("td")
                val mlbId = mlbIdFromMlbUrl(reportColumns(1))
                val injuryReportDate = reportColumns(2).getAttribute("textContent")
                val status = reportColumns(3).getAttribute("textContent")
                val dueBack = reportColumns(4).getAttribute("textContent")
                val injury = reportColumns(5).getAttribute("textContent")
                InjuryReport(mlbId, reportTimeString, injuryReportDate, status, dueBack, injury)
              }
            }
          }
        }
        case _ => { logger.info("No injuries to report"); List.empty[InjuryReport] }
      }
    }
    case _ => throw new IllegalStateException("No `dataTable' found")
  })

  logger.info(injuries.size + " injuries processed")

  quit
}
