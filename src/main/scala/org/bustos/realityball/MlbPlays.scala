package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
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

  case class HalfInning(inning: String, side: String)
  case class Play(inning: String, side: String, id: String, count: String, pitches: String, play: String)

}

class MlbPlays(game: Game) extends Chrome {

  import MlbPlays._

  implicitlyWait(Span(20, Seconds))

  val realityballData = new RealityballData
  val runners = new MlbRunnerPositions
  val logger = LoggerFactory.getLogger(getClass)

  val date = CcyymmddSlashDelimFormatter.parseDateTime(game.date)
  val mlbHomeTeam = realityballData.mlbComIdFromRetrosheet(game.homeTeam)
  val mlbVisitingTeam = realityballData.mlbComIdFromRetrosheet(game.visitingTeam)

  logger.info("********************************")
  logger.info("*** Retrieving play results for " + game.visitingTeam + " @ " + game.homeTeam + " on " + CcyymmddDelimFormatter.print(date))
  logger.info("********************************")

  val host = GamedayURL
  go to host + "mlb/gameday/index.jsp?gid=" + CcyymmddDelimFormatter.print(date) + "_" + game.visitingTeam.toLowerCase + "mlb_" + game.homeTeam.toLowerCase + "mlb_1&mode=plays"

  var pitchCount = 0

  def sideFromElement(halfInning: RemoteWebElement): HalfInning = {
    halfInning.findElementByTagName("h3") match {
      case side: RemoteWebElement => {
        val description = side.getAttribute("textContent").split("\n")(1)
        logger.info("*** " + description)
        if (description.contains("Top")) HalfInning(description.split(" ")(1), "0")
        else HalfInning(description.split(" ")(1), "1")
      }
      case _ => throw new IllegalStateException("Could not determine half-inning")
    }
  }

  def pitchResult(runningPitches: String, pitch: WebElement): String = {
    if (pitch.getTagName == "td") {
      pitchCount = pitchCount + 1
      val pitchDescription = pitch.getAttribute("textContent").toLowerCase
      logger.info(pitchCount + " " + pitchDescription)
      if (pitchDescription == "ball" || pitchDescription == "ball in dirt" || pitchDescription == "pitchout") runningPitches + "B"
      else if (pitchDescription == "called strike") runningPitches + "C"
      else if (pitchDescription == "foul" || pitchDescription == "foul (runner going)" || pitchDescription == "foul bunt") runningPitches + "F"
      else if (pitchDescription.contains("swinging strike")) runningPitches + "S"
      else if (pitchDescription == "foul tip") runningPitches + "T"
      else if (pitchDescription == "hit by pitch") runningPitches + "H"
      else if (pitchDescription == "intent ball") runningPitches + "I"
      else if (pitchDescription == "missed bunt") runningPitches + "MB"
      else if (pitchDescription.contains("in play")) runningPitches + "X"
      else {
        logger.warn("UNKOWN PITCH " + pitchDescription)
        runningPitches
      }
    } else runningPitches
  }

  def inPlayModifier(ballType: String, playDescription: String): String = {
    if (playDescription.contains(ballType + "pitcher")) "1"
    else if (playDescription.contains(ballType + "catcher")) "2"
    else if (playDescription.contains(ballType + "first baseman")) "3"
    else if (playDescription.contains(ballType + "second baseman")) "4"
    else if (playDescription.contains(ballType + "shortstop")) "5"
    else if (playDescription.contains(ballType + "third baseman")) "6"
    else if (playDescription.contains(ballType + "left fielder")) "7"
    else if (playDescription.contains(ballType + "center fielder")) "8"
    else if (playDescription.contains(ballType + "right fielder")) "9"
    else "0"
  }

  def atBatAdvancements(atBatResult: RemoteWebElement, batter: Player, team: String): String = {
    runners.advancementString(atBatResult.getAttribute("textContent").toLowerCase.replace("\n", ""), batter, team, YearFormatter.print(date))
  }

  def atBatPlayString(atBatResult: RemoteWebElement): String = {
    val playDescription = atBatResult.getAttribute("textContent").toLowerCase
    if (playDescription.contains("single")) "S"
    else if (playDescription.contains("double")) "D"
    else if (playDescription.contains("triple")) "T"
    else if (playDescription.contains("homers")) "HR"
    else if (playDescription.contains("hits a grand slam")) "HR"
    else if (playDescription.contains("strikes out") || playDescription.contains("called out on strikes")) "K"
    else if (playDescription.contains("flies out")) inPlayModifier("flies out to ", playDescription) + "/F"
    else if (playDescription.contains("pops out")) inPlayModifier("pops out to ", playDescription) + "/P"
    else if (playDescription.contains("reaches on a fielder's choice")) inPlayModifier("reaches on a fielder's choice, ", playDescription) + "/P"
    else if (playDescription.contains("reaches on catcher interference")) "C/E2"
    else if (playDescription.contains("pops into a force out, ")) inPlayModifier("pops into a force out, ", playDescription) + "/P"
    else if (playDescription.contains("grounds out")) inPlayModifier("grounds out, ", playDescription) + "/G"
    else if (playDescription.contains("ground bunts into a force out")) inPlayModifier("ground bunts into a force out, , ", playDescription) + "/G"
    else if (playDescription.contains("grounds into")) "0/G"
    else if (playDescription.contains("out on a sacrifice bunt")) "0/SH"
    else if (playDescription.contains("hits a sacrifice bunt")) "0/SH"
    else if (playDescription.contains("out on a sacrifice fly")) inPlayModifier("out on a sacrifice fly to ", playDescription) + "/SF"
    else if (playDescription.contains("lines out")) inPlayModifier("lines out to ", playDescription) + "/L"
    else if (playDescription.contains("lines into a force out")) inPlayModifier("lines into a force out, ", playDescription) + "/L"
    else if (playDescription.contains("caught stealing 2nd base")) "CS2"
    else if (playDescription.contains("caught stealing 3rd base")) "CS3"
    else if (playDescription.contains(" hit by pitch.")) "HP"
    else if (playDescription.contains(" walks.")) "W"
    else if (playDescription.contains(" intentionally walks ")) "I"
    else if (playDescription.contains("reaches on a fielding error") || playDescription.contains("reaches on a throwing error") || playDescription.contains("reaches on a force attempt")) "E"
    else {
      logger.warn("UNKOWN PLAY " + playDescription)
      ""
    }
  }

  def player(playerElement: RemoteWebElement, tag: String, team: String): Player = {

    playerElement.findElementByClassName(tag) match {
      case foundPlayer: RemoteWebElement => {
        val lastName = runners.lastName(foundPlayer.getAttribute("textContent").split("\n")(2))
        val firstName = if (tag == "plays-atbat-batter") {
          val batterNameExpression = (""".*\.(.*walks )?(.*upheld: )?([A-Z].*?) """ + lastName + """(\.)?(.*)""").r
          val textContent = playerElement.getAttribute("textContent").replaceAll("\n", "")
          println(textContent)
          textContent match {
            case batterNameExpression(walks, upheld, fName, place4, place5) => fName
            case _ => ""
          }
        } else ""
        runners.playerFromString(firstName + " " + lastName, team, YearFormatter.print(date))
      }
      case _ => throw new IllegalStateException("Could not find " + tag)
    }
  }

  def playForAtBat(atBat: RemoteWebElement, inningAndSide: HalfInning): Play = {
    val team = {
      if (inningAndSide.side == "1") game.homeTeam else game.visitingTeam
    }
    val elementClass = atBat.getAttribute("class")
    if (elementClass.contains("plays-atbat") && !elementClass.contains("-pitcher") && !elementClass.contains("-batter")) {
      val batter = player(atBat, "plays-atbat-batter", team)
      val play = atBat.findElementByTagName("dt") match {
        case atBatResult: RemoteWebElement => {
          atBatPlayString(atBatResult) + atBatAdvancements(atBatResult, batter, team)
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
      val latestPlay = Play(inningAndSide.inning, inningAndSide.side, batter.id, "00", atBatPitches, play)
      logger.info("Play: " + latestPlay)
      latestPlay
    } else {
      val playDescription = atBat.getAttribute("textContent").replaceAll("\n", "")
      val pitcherNameExpression = """Pitching Change: (.*) replaces.*""".r
      playDescription match {
        case pitcherNameExpression(name) => {
          val team = if (inningAndSide.side == "1") game.visitingTeam else game.homeTeam
          val player = runners.playerFromString(name, team, YearFormatter.print(date))
          if (player.position != "P") logger.warn(player + " is not a pitcher")
          val latestPlay = if (inningAndSide.side == "1") Play(inningAndSide.inning, "0", player.id, "00", "\"" + player.firstName + " " + player.lastName + "\"", "sub")
          else Play(inningAndSide.inning, "1", player.id, "00", "\"" + player.firstName + " " + player.lastName + "\"", "sub")
          logger.info("Pitching Change: " + latestPlay)
          latestPlay
        }
        case _ => null //throw new IllegalStateException("Could not determine pitcher's name: " + playDescription)
      }
    }
  }

  def halfInningPlays(halfInning: RemoteWebElement, side: HalfInning): List[Play] = {

    def processAtBats(atBats: List[WebElement], collection: List[Play]): List[Play] = {
      if (atBats == Nil) collection
      else {
        atBats.head match {
          case x: RemoteWebElement => processAtBats(atBats.tail, playForAtBat(x, side) :: collection)
          case _                   => collection
        }
      }
    }

    halfInning.findElementsByTagName("li") match {
      case atBats: java.util.List[WebElement] => processAtBats(atBats.toList, List.empty[Play]).filter { _ != null }
      case _                                  => logger.warn("NO AT BATS FOUND"); List.empty[Play]
    }
  }

  def processedHalfInnings(plays: RemoteWebElement): Map[HalfInning, List[Play]] = {

    def processHalfInning(halfInning: List[WebElement], collection: Map[HalfInning, List[Play]]): Map[HalfInning, List[Play]] = {
      if (halfInning == Nil) collection
      else {
        runners.clear
        halfInning.head match {
          case x: RemoteWebElement => {
            val side = sideFromElement(x)
            processHalfInning(halfInning.tail, collection + (side -> halfInningPlays(x, side)))
          }
          case _ => collection
        }
      }
    }

    plays.findElementsByClassName("plays-half-inning") match {
      case x: java.util.List[WebElement] => processHalfInning(x.toList, Map.empty[HalfInning, List[Play]])
      case _                             => logger.warn("NO HALF INNINGS FOUND"); Map.empty[HalfInning, List[Play]]
    }
  }

  val plays: Map[HalfInning, List[Play]] = {
    find("plays") match {
      case Some(x) => x.underlying match {
        case plays: RemoteWebElement => processedHalfInnings(plays)
        case _                       => Map.empty[HalfInning, List[Play]]
      }
      case _ => logger.warn("NO PLAYS FOUND"); Map.empty[HalfInning, List[Play]]
    }
  }

  def playStrings: List[String] = {
    def eventString(play: Play): String = {
      if (play.play == "sub") {
        "sub," + play.id + "," + play.pitches + "," + play.side + ",0,1\n"
      } else "play," + play.inning + "," + play.side + "," + play.id + "," + play.count + "," + play.pitches + "," + play.play + "\n"
    }

    plays.toList.sortBy(i => (i._1.inning, i._1.side)).foldLeft(List.empty[String])({ (x, y) => x ++ y._2.map { eventString(_) } }).toList
  }

  quit
}
