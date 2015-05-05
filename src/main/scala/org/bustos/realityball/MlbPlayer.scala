package org.bustos.realityball

import java.io._
import org.joda.time._
import org.joda.time.format._
import org.scalatest._
import selenium._
import org.scalatest.time.{ Span, Seconds }
import org.openqa.selenium.support.ui.{ WebDriverWait, ExpectedCondition }
import org.openqa.selenium._
import org.openqa.selenium.By._
import remote._
import htmlunit._
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import RealityballRecords._
import RealityballConfig._

class MlbPlayer(val mlbId: String) extends Chrome {

  implicitlyWait(Span(20, Seconds))

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)

  val nameExpression = """(.*) (.*?)[0-9].*""".r
  val nameNoNumberExpression = """(.*) (.*?)\|.*""".r
  val positionExpression = """\|(.*)""".r
  val logoTeamName = """.*/images/logos/30x34/(.*)_logo.png""".r

  logger.info("*****************************************")
  logger.info("*** Retrieving player info for " + mlbId + " ***")
  logger.info("*****************************************")

  val fileName = DataRoot + "gamedayPages/players/" + mlbId + ".html"

  if (new File(fileName).exists) {
    val caps = DesiredCapabilities.chrome
    caps.setCapability("chrome.switches", Array("--disable-javascript"))
    go to "file://" + fileName
  } else {
    val host = MlbURL
    go to host + "team/player.jsp?player_id=" + mlbId
  }

  val playerName = {
    find("player_name") match {
      case Some(x) => x.underlying match {
        case name: RemoteWebElement => {
          name.getAttribute("textContent").replaceAll("\u00A0","") match {
            case nameExpression(firstName, lastName) => (firstName, lastName)
            case nameNoNumberExpression(firstName, lastName) => (firstName, lastName)
          }
        }
      }
      case None => throw new Exception("Could not find player name")
    }
  }

  val bats = {
    find("player_bats") match {
      case Some(x) => x.underlying match {
        case name: RemoteWebElement => name.getAttribute("textContent")
      }
      case None => throw new Exception("Could not find player bats")
    }
  }

  val throws = {
    find("player_throws") match {
      case Some(x) => x.underlying match {
        case name: RemoteWebElement => name.getAttribute("textContent")
      }
      case None => throw new Exception("Could not find player throws")
    }
  }

  val position = {
    find("player_position") match {
      case Some(x) => x.underlying match {
        case name: RemoteWebElement => name.getAttribute("textContent").replaceAll("\u00A0","") match {
          case positionExpression(positionString) => positionString
        }
      }
      case None => throw new Exception("Could not find player position")
    }
  }

  val team = {
    find("main_name") match {
      case Some(x) => x.underlying match {
        case panel: RemoteWebElement => {
          panel.findElementByClassName("logo").getAttribute("src") match {
            case logoTeamName(teamName) => teamName.toUpperCase
          }
        }
      }
      case None => throw new Exception("Could not find career panel")
    }
  }

  val player = Player(mlbId, "2015", playerName._2, playerName._1, bats, throws, team, position)

  if (!(new File(fileName)).exists) {
    val writer = new FileWriter(new File(fileName))
    writer.write(pageSource)
    writer.close
  }
}
