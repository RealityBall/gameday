package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
import java.io._
import org.scalatest._
import selenium._
import org.scalatest.time.{ Span, Seconds }
import org.openqa.selenium._
import remote._
import org.slf4j.LoggerFactory
import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.RealityballConfig._
import scala.collection.JavaConversions._
import org.bustos.realityball.common.RealityballData

class FantasyAlarmLineups(val date: DateTime) extends Chrome {

  implicitlyWait(Span(20, Seconds))

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)

  val teamExpression = """http://www.fantasyalarm.com/teams/mlb/(.*)/""".r
  val playerExpression = """http://www.fantasyalarm.com/players/mlb/(.*)/(.*)/""".r
  val nameExpression = """(.*?) (.*)""".r
  val EmptyLineupEntry = Lineup("", new DateTime, "", "", 0, "", Some(0.0), Some(0.0), Some(0.0))

  logger.info("*****************************************")
  logger.info("*** Retrieving lineups for " + CcyymmddDelimFormatter.print(date) + " ***")
  logger.info("*****************************************")

  val fileName = DataRoot + "fantasyAlarmPages/" + date.getYear + "/" + CcyymmddFormatter.print(date) + ".html"

  if (new File(fileName).exists) {
    val caps = DesiredCapabilities.chrome;
    caps.setCapability("chrome.switches", Array("--disable-javascript"));
    go to "file://" + fileName
  } else {
    val host = FantasyAlarmURL
    go to host + "lineups.php?lineup_date=" + date.getYear + "-" + date.getMonthOfYear + "-" + date.getDayOfMonth + "&sport=mlb"
    if (!(new File(fileName)).exists) {
      val writer = new FileWriter(new File(fileName))
      writer.write(pageSource.replaceAll("<meta http-equiv=\"REFRESH\" content=\"300\" />", "<meta http-equiv=\"REFRESH\" content=\"9999\" />"))
      writer.close
    }
    val caps = DesiredCapabilities.chrome;
    caps.setCapability("chrome.switches", Array("--disable-javascript"));
    go to "file://" + fileName
  }
  if ((new File(fileName)).exists) {
    new File(fileName).delete
  }

  def salary(element: WebElement): Option[Double] = {
    val value = textContent(element)
    if (value.contains("$")) {
      Some(value.replace("$", "").toDouble)
    } else None
  }

  def textContent(element: WebElement): String = {
    element.getAttribute("textContent")
  }

  def playerFromId(faName: String, faTeam: String): Player = {
    faName.toLowerCase.replaceAll("a-j-", "a.j. ").replaceAll("j-d-", "j.d. ").replaceAll("c-j-", "c.j. ").replaceAll("-d-", " d'").replaceAll("-", " ").replaceAll(" jr", "") match {
      case nameExpression(first, last) => {
        try {
          realityballData.playerFromName(first, last, "", "")
        } catch {
          case e: Throwable => realityballData.playerFromName(first, last, "2014", faTeam)
        }
      }
      case _ => throw new Exception("Unable to parse name")
    }
  }

  val lineups: Map[Team, List[Lineup]] = {
    findAll(className("lineup-div5")).map({ f =>
      val team = f.underlying.findElement(By.className("team-name")).getAttribute("href") match {
        case teamExpression(teamName) => realityballData.teamFromName(teamName.replace("-", " "))
        case _ => throw new IllegalStateException("Unable to determine team")
      }
      logger.info("Lineups for " + team.mlbComName)
      (team -> f.underlying.findElements(By.tagName("tr")).map({
        case wE: RemoteWebElement => {
          val columns = wE.findElements(By.tagName("td"))
          if (!columns.isEmpty) {
            val playerID = columns(2).findElement(By.tagName("a")).getAttribute("href") match {
              case playerExpression(id, name) => (id, name)
              case _ => ("", "")
            }
            val player = playerFromId(playerID._2, team.mlbComId)
            val lineupPosition = if (textContent(columns(0)) != "") textContent(columns(0)).toInt else 0
            val position = textContent(columns(1))
            if (position == "") EmptyLineupEntry
            else Lineup(player.id, date, "game", team.mlbComName, lineupPosition, position, salary(columns(8)), salary(columns(9)), salary(columns(10)))
          } else EmptyLineupEntry
        }
        case _ => EmptyLineupEntry
      }).toList.filter(_.mlbId != ""))
    }).toMap
  }

  quit
}