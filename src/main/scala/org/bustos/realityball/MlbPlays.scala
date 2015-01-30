package org.bustos.realityball

import java.util.Date
import java.text.SimpleDateFormat
import org.scalatest._
import selenium._
import org.scalatest.time.{ Span, Seconds }
import org.openqa.selenium.support.ui.{ WebDriverWait, ExpectedCondition }
import org.openqa.selenium._
import remote._
import htmlunit._
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import RealityballRecords._
import RealityballConfig._

object MlbPlays {
  case class Play(inning: String, side: String, id: String, count: String, pitches: String, play: String)

  val dateFormat = new SimpleDateFormat("yyyy_MM_dd");
  val yearFormat = new SimpleDateFormat("yyyy");
}

class MlbPlays(date: Date, awayTeam: String, homeTeam: String) extends Chrome {

  import MlbPlays._

  implicitlyWait(Span(20, Seconds))

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)
  logger.info("********************************")
  logger.info("*** Retrieving play results for " + awayTeam + " @ " + homeTeam + " on " + dateFormat.format(date))
  logger.info("********************************")

  val host = GamedayURL
  go to host + "mlb/gameday/index.jsp?gid=" + dateFormat.format(date) + "_" + awayTeam.toLowerCase + "mlb_" + homeTeam.toLowerCase + "mlb_1&mode=plays"

  var pitchCount = 0
  var plays = List.empty[Play]

  def pitchResult(runningPitches: String, pitch: WebElement): String = {
    if (pitch.getTagName == "td") {
      pitchCount = pitchCount + 1
      val pitchDescription = pitch.getAttribute("textContent").toLowerCase
      logger.info(pitchCount + " " + pitchDescription)
      if (pitchDescription == "ball" || pitchDescription == "ball in dirt") runningPitches + "B"
      else if (pitchDescription == "called strike") runningPitches + "C"
      else if (pitchDescription == "foul") runningPitches + "F"
      else if (pitchDescription.contains("swinging strike")) runningPitches + "S"
      else if (pitchDescription == "foul tip") runningPitches + "T"
      else if (pitchDescription.contains("in play")) runningPitches + "X"
      else {
        logger.warn("UNKOWN PITCH " + pitchDescription)
        runningPitches
      }
    } else runningPitches
  }

  def atBatPlay(atBatResult: RemoteWebElement): String = {
    val playDescription = atBatResult.getAttribute("textContent").toLowerCase
    if (playDescription.contains("single")) "S"
    else if (playDescription.contains("double")) "D"
    else if (playDescription.contains("triple")) "T"
    else if (playDescription.contains("strikes out") ||
      playDescription.contains("called out on strikes")) "K"
    else if (playDescription.contains("flies out") ||
      playDescription.contains("grounds out") ||
      playDescription.contains("pops out") ||
      playDescription.contains("lines out") ||
      playDescription.contains("grounds into")) "9"
    else if (playDescription.contains("reaches on a fielding error")) "E"
    else {
      logger.warn("UNKOWN PLAY " + playDescription)
      ""
    }
  }

  def player(player: RemoteWebElement, tag: String): Player = {
    player.findElementByClassName(tag) match {
      case foundPlayer: RemoteWebElement => {
        val playerName = foundPlayer.getAttribute("textContent").split("\n")(2).split(" ")
        realityballData.playerFromName(playerName(0)(0).toString, playerName(1), yearFormat.format(date))
      }
      case _ => throw new IllegalStateException("Could not find " + tag)
    }
  }

  def processAtBat(atBat: RemoteWebElement, inningAndSide: (String, String)) = {
    val pitcher = player(atBat, "plays-atbat-pitcher")
    val batter = player(atBat, "plays-atbat-batter")
    val play = atBat.findElementByTagName("dt") match {
      case atBatResult: RemoteWebElement => {
        atBatPlay(atBatResult)
      }
      case _ => {
        logger.warn("NO AT BAT RESULT FOUND")
        ""
      }
    }
    val atBatPitches = atBat.findElementsByClassName("plays-pitch-result") match {
      case pitches: java.util.List[WebElement] => {
        pitches.foldLeft("")((x, y) => pitchResult(x, y))
      }
      case _ => {
        logger.warn("NO PITCHES FOUND")
        ""
      }
    }
    val latestPlay = Play(inningAndSide._1, inningAndSide._2, batter.id, "00", atBatPitches, play)
    logger.info("Play: " + latestPlay)
    plays = latestPlay :: plays
  }

  def inningAndSide(halfInning: RemoteWebElement): (String, String) = {
    halfInning.findElementByTagName("h3") match {
      case side: RemoteWebElement => {
        val description = side.getAttribute("textContent").split("\n")(1)
        logger.info("*** " + description)
        if (description.contains("Top")) (description.split(" ")(1), "0")
        else (description.split(" ")(1), "1")
      }
      case _ => throw new IllegalStateException("Could not determine half-inning")
    }
  }

  def processHalfInning(halfInning: RemoteWebElement) = {
    halfInning.findElementsByClassName("plays-atbat") match {
      case atBats: java.util.List[WebElement] => {
        atBats.toList.foreach {
          _ match {
            case atBat: RemoteWebElement => processAtBat(atBat, inningAndSide(halfInning))
            case _                       => logger.warn("INVALID AT BAT")
          }
        }
      }
      case _ => logger.warn("NO AT BATS FOUND")
    }
  }

  def processHalfInnings(plays: RemoteWebElement) = {
    plays.findElementsByClassName("plays-half-inning") match {
      case halfInnings: java.util.List[WebElement] => {
        halfInnings.toList.foreach {
          _ match {
            case halfInning: RemoteWebElement => processHalfInning(halfInning)
            case _                            => logger.warn("INVALID HALF INNING")
          }
        }
      }
      case _ => logger.warn("NO HALF INNINGS FOUND")
    }
  }

  find("plays") match {
    case Some(x) => x.underlying match {
      case plays: RemoteWebElement => processHalfInnings(plays)
      case _                       =>
    }
    case _ => logger.warn("NO PLAYS FOUND")
  }
  quit
}
