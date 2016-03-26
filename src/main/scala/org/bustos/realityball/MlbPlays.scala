package org.bustos.realityball

import java.io._

import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballData
import org.bustos.realityball.common.RealityballRecords._
import org.openqa.selenium.By.{ByClassName, ByTagName}
import org.openqa.selenium._
import org.openqa.selenium.interactions.Actions
import org.openqa.selenium.remote._
import org.scalatest._
import org.scalatest.selenium._
import org.scalatest.time.{Seconds, Span}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object MlbPlays {

  case class HalfInning(inning: String, side: String)
  case class Play(inning: String, side: String, id: String, count: String, pitches: String, play: String)

  val pinchRunnerExpression = """.*Pinch-runner (.*) replaces (.*)\..*""".r
  val wildPitch = """.*With (.*) batting, wild pitch.*""".r
  val passedBall = """.*With (.*) batting, passed ball by .*""".r
  val interferenceError = """Field Error(.*) reaches on an interference error.*""".r
  val caughtStealing = """.*With (.*) batting, (.*) caught stealing.*""".r

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

  val version_2_format = CcyymmddFormatter.print(date) > "20150401"
  val pitcherChangeExpression = {
    if (version_2_format) """.*Pitching Change: (.*) replaces.*""".r
    else """Pitching Change: (.*) replaces.*""".r
  }
  logger.info("***********************************************************")
  logger.info("*** Retrieving play results for " + game.visitingTeam + " @ " + game.homeTeam + " on " + CcyymmddDelimFormatter.print(date) + " ***")
  logger.info("***********************************************************")

  val fileName = DataRoot + "gamedayPages/" + date.getYear + "/" + game.visitingTeam + "_" + game.homeTeam + "_" + CcyymmddFormatter.print(date) + "_play_by_play.html"

  if (new File(fileName).exists) {
    val caps = DesiredCapabilities.chrome
    caps.setCapability("chrome.switches", Array("--disable-javascript"))
    go to "file://" + fileName
  } else {
    val host = GamedayURL

    def constructVersion2Page = {
      go to host + "mlb/gameday/index.jsp?gid=" + CcyymmddDelimFormatter.print(date) + "_" + game.visitingTeam.toLowerCase + "mlb_" + game.homeTeam.toLowerCase + "mlb_1&mode=plays"
      (1 to 20).foreach({ inningNo =>
        find("plays") match {
          case Some(x) => x.underlying match {
            case plays: RemoteWebElement => {
              val inningElement = plays.findElementByClassName("inning_nav")
              Thread sleep 1000
              inningElement.findElements(new ByTagName("h5")) match {
                case innings: java.util.List[WebElement] => {
                  innings.filter(_.getAttribute("textContent").toInt == inningNo).foreach({ x =>
                    val tries = 20
                    (1 to tries).takeWhile({ y =>
                      val webBuilder = new Actions(webDriver)
                      try {
                        webBuilder.click(x).build.perform
                      } catch {
                        case _: Exception =>
                      }
                      Thread sleep 2000
                      plays.findElementsByClassName("inning_tabs") match {
                        case x: java.util.List[WebElement] => {
                          val inningFound = x.toList.foldLeft(true)((result, element) => element match {
                            case inning: RemoteWebElement => {
                              try {
                                result && !inning.getAttribute("textContent").toLowerCase.contains("top " + inningNo)
                              } catch {
                                case e: Exception => true
                              }
                            }
                            case _ => true
                          })
                          if (inningFound && y == tries) {
                            throw new IllegalStateException("Inning " + inningNo + " never materialized")
                          } else if (!inningFound) {
                            print(inningNo)
                          }
                          inningFound
                        }
                        case _ => logger.warn("NO HALF INNINGS FOUND"); false
                      }
                    })
                  })
                }
                case _ => logger.warn("NO INNINGS FOUND")
              }
            }
            case _ =>
          }
          case _ =>
        }
      })
    }

    if (version_2_format) {
      try {
        constructVersion2Page
      } catch {
        case _: Exception => constructVersion2Page
      }
    } else {
      go to host + "mlb/gameday/index.jsp?gid=" + CcyymmddDelimFormatter.print(date) + "_" + game.visitingTeam.toLowerCase + "mlb_" + game.homeTeam.toLowerCase + "mlb_1,game_state=Wrapup,game_tab=play-by-play"
      println
    }
  }

  var pitchCount = 0
  var lastSideBottom = true
  var lastInning = 0

  def sideFromElement(halfInning: RemoteWebElement): HalfInning = {

    def validateSide(description: String) = {
      if (description.toLowerCase.contains("top")) {
        if (description.contains((lastInning + 1).toString)) lastInning = lastInning + 1
        else throw new IllegalStateException("Inning skipped")
        if (lastSideBottom) lastSideBottom = !lastSideBottom
        else throw new IllegalStateException("Side inconsistent")
      } else {
        if (!lastSideBottom) lastSideBottom = !lastSideBottom
        else throw new IllegalStateException("Side inconsistent")
      }
    }

    if (version_2_format) {
      halfInning.findElementByTagName("h2") match {
        case side: RemoteWebElement => {
          val description = side.getAttribute("textContent")
          logger.info("*** " + description)
          validateSide(description)
          if (description.toLowerCase.contains("top")) {
            HalfInning(description.split(" ")(1).charAt(0).toString, "0")
          } else {
            HalfInning(description.split(" ")(1).charAt(0).toString, "1")
          }
        }
        case _ => throw new IllegalStateException("Could not determine half-inning")
      }
    } else {
      halfInning.findElementByTagName("h3") match {
        case side: RemoteWebElement => {
          val description = side.getAttribute("textContent").split("\n")(1)
          logger.info("*** " + description)
          validateSide(description)
          if (description.contains("Top")) HalfInning(description.split(" ")(1), "0")
          else HalfInning(description.split(" ")(1), "1")
        }
        case _ => throw new IllegalStateException("Could not determine half-inning")
      }
    }
  }

  def pitchResult(runningPitches: String, pitch: WebElement): String = {
    if (pitch.getTagName == "td" || (version_2_format && pitch.getTagName == "span")) {
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
        logger.warn("UNKNOWN PITCH " + pitchDescription)
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
    if (atBatResult.getAttribute("textContent").toLowerCase.contains("interference")) {
      println("")
    }
    runners.advancementString(atBatResult.getAttribute("textContent").toLowerCase.
      replace("\n", "").
      replace("fielder steven souza jr.", "fielder steven souza.").
      replace(".   c.  j.   cron ", ". c cron").
      replace(".    c.  j.   cron", ".  c cron").
      replace(".  j.   cron", ". c cron").
      replace(".    j.  t.   realmuto", ". j realmuto").
      replace(".   j.  t.   realmuto", ". j realmuto").
      replace(".  t.   realmuto", ". j realmuto").
      replace("scores.   realmuto", "scores.  j realmuto").
      replace(".   j.  d.   martinez", ". j martinez").
      replace(".  j.  d.   martinez", ". j martinez").
      replace(".    j.  d.   martinez", ". j martinez").
      replace(".  d.   martinez", ". j martinez").
      replace("scores.   pierzynski", "scores. a pierzynski").
      replace("kemp.   l.   hoes", "kemp.  l hoes").
      replace(" juan carlos", " juan").replace(" john ryan", " john").
      replace(" jr.", "").
      replace (".    j.  d.    yoenis cespedes", ".  j martinez scores.  yoenis cespedes").
      replace(" a.  j.", " a").replace(" c.  j.", " c").replace(" b.  j.", " b").
      replace(" l.  j.", " l").replace(" t.  j.", " t").replace(" j.  p.", " j").
      replace(" j.  d.", " j").replace("ramirez.   martinez scores.", "ramirez.  j martinez scores.").
      replace(" j.", " j").replace(" b.", " b").replace(" a.", " a").replace(" c.", " c"),
      batter, team, YearFormatter.print(date))
  }

  def atBatPlayString(atBatResult: RemoteWebElement): String = {
    val playDescription = atBatResult.getAttribute("textContent").toLowerCase
    if (playDescription.contains("single")) "S"
    else if (playDescription.contains("double")) "D"
    else if (playDescription.contains("triple")) "T"
    else if (playDescription.contains("homers")) "HR"
    else if (playDescription.contains("hits an inside-the-park home run")) "HR"
    else if (playDescription.contains("hits a grand slam")) "HR"
    else if (playDescription.contains(" hit by pitch.")) "HP"
    else if (playDescription.contains(" walks.")) "W"
    else if (playDescription.contains(" intentionally walks ")) "I"
    else if (playDescription.contains("reaches on a fielding error") || playDescription.contains("reaches on a throwing error") || playDescription.contains("reaches on a force attempt")) "E"
    else if (playDescription.contains("strikes out") || playDescription.contains("called out on strikes")) "K"
    else if (playDescription.contains("flies out")) inPlayModifier("flies out to ", playDescription) + "/F"
    else if (playDescription.contains("pops out")) inPlayModifier("pops out to ", playDescription) + "/P"
    else if (playDescription.contains("reaches on a fielder's choice")) inPlayModifier("reaches on a fielder's choice, ", playDescription) + "/P"
    else if (playDescription.contains("reaches on catcher interference")) "C/E2"
    else if (playDescription.contains("reaches on an interference error by pitcher")) "E1"
    else if (playDescription.contains("reaches on a missed catch error by pitcher")) "E1"
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
    else if (playDescription.contains("caught stealing home")) "CSH"
    else if (playDescription.contains("wild pitch") && playDescription.contains("out at home")) "CSH"
    else if (playDescription.contains("picks off")) "/PO"
    else {
      logger.warn("UNKNOWN PLAY " + playDescription)
      "NP"
    }
  }

  def batterForPlay(playerElement: RemoteWebElement, team: String): Player = {

    if (version_2_format) {
      playerElement.findElementByTagName("a") match {
        case foundPlayer: RemoteWebElement => {
          val name = foundPlayer.getAttribute("href")
          runners.playerFromMlbId(name.split("=").last, YearFormatter.print(date))
        }
      }
    } else {
      playerElement.findElementByClassName("plays-atbat-batter") match {
        case foundPlayer: RemoteWebElement => {
          val lastName = runners.lastName(foundPlayer.getAttribute("textContent").split("\n")(2))
          val firstName = {
            val batterNameExpression = (""".*\.(With )?(.*walks )?(.*upheld: )?(.*overturned: )?(With )?([A-Z].*?) """ + lastName + """(\.)?(.*)""").r
            val textContent = playerElement.getAttribute("textContent").replaceAll("\n", "").split("Pitcher")(0)
            logger.info("Play text: " + textContent)
            textContent match {
              case batterNameExpression(withT, walks, upheld, overturned, challengeWith, fName, place4, place5) => fName
              case _ => ""
            }
          }
          if (firstName == "") {
            logger.warn("Batter was not involved in play")
            null
          } else runners.playerFromString(firstName + " " + lastName, team, YearFormatter.print(date))
        }
        case _ => throw new IllegalStateException("Could not find 'plays-atbat-batter'")
      }
    }
  }

  def wildPitchPlay(playDescription: String, name: String, batterTeam: String, inningAndSide: HalfInning): Play = {
    val player = runners.playerFromString(name, batterTeam, YearFormatter.print(date))
    val advancements = runners.advancementString(
      playDescription.toLowerCase
        .replace(" jr.", "")
        .replace(", j.  b.   shuck", ". j shuck")
        .replace(", j.  d.   martinez", ". j martinez")
        .replace(".    j.  t.   realmuto", ".  j realmuto")
        .replace(", j.   realmuto", ".  j realmuto"),
      player, batterTeam, YearFormatter.print(date))
    val latestPlay = Play(inningAndSide.inning, inningAndSide.side, player.id, "00", "\"" + player.firstName + " " + player.lastName + "\"", "WP" + advancements)
    logger.info("Play: " + latestPlay)
    latestPlay
  }

  def playForAtBat(atBat: RemoteWebElement, inningAndSide: HalfInning): Play = {
    val elementClass = atBat.getAttribute("class")
    val batterTeam: String = { if (inningAndSide.side == "1") game.homeTeam else game.visitingTeam }
    val pitcherTeam = { if (inningAndSide.side == "1") game.visitingTeam else game.homeTeam }
    val play = {
      if (version_2_format) {
        elementClass.contains("play")
      } else {
        elementClass.contains("plays-atbat") && !elementClass.contains("-pitcher") && !elementClass.contains("-batter")
      }
    }

    if (play) {
      val batter = batterForPlay(atBat, batterTeam)
      if (batter == null) null
      else {
        val play = if (version_2_format) {
          atBat.findElementByClassName("description") match {
            case atBatResult: RemoteWebElement => {
              atBatPlayString(atBatResult) + atBatAdvancements(atBatResult, batter, batterTeam)
            }
            case _ => {
              logger.warn("NO AT BAT RESULT FOUND")
              ""
            }
          }
        } else {
          atBat.findElementByTagName("dt") match {
            case atBatResult: RemoteWebElement => {
              atBatPlayString(atBatResult) + atBatAdvancements(atBatResult, batter, batterTeam)
            }
            case _ => {
              logger.warn("NO AT BAT RESULT FOUND")
              ""
            }
          }
        }
        val atBatPitches = if (version_2_format) {
          atBat.findElementsByClassName("call-description") match {
            case pitches: java.util.List[WebElement] => {
              pitches.foldLeft("")((x, y) => pitchResult(x, y))
            }
            case _ => {
              logger.warn("NO PITCHES FOUND")
              ""
            }
          }
        } else {
          atBat.findElementsByClassName("plays-pitch-result") match {
            case pitches: java.util.List[WebElement] => {
              pitches.foldLeft("")((x, y) => pitchResult(x, y))
            }
            case _ => {
              logger.warn("NO PITCHES FOUND")
              ""
            }
          }
        }
        val latestPlay = Play(inningAndSide.inning, inningAndSide.side, batter.id, "00", atBatPitches, play)
        logger.info("Play: " + latestPlay)
        latestPlay
      }
    } else {
      // Play actions
      val playDescription = atBat.getAttribute("textContent").replaceAll("\n", "")
      playDescription match {
        case pitcherChangeExpression(name) => {
          val player = runners.playerFromString(name, pitcherTeam, YearFormatter.print(date))
          if (player.position != "P") logger.warn(player + " is not a pitcher")
          val latestPlay = if (inningAndSide.side == "1") Play(inningAndSide.inning, "0", player.id, "00", "\"" + player.firstName + " " + player.lastName + "\"", "sub")
          else Play(inningAndSide.inning, "1", player.id, "00", "\"" + player.firstName + " " + player.lastName + "\"", "sub")
          logger.info("Pitching Change: " + latestPlay)
          latestPlay
        }
        case pinchRunnerExpression(in, out) => {
          runners.pinchRunner(in, out, batterTeam, YearFormatter.print(date))
          null // TO DO: Generate substitution line
        }
        case wildPitch(name) => {
          wildPitchPlay(playDescription, name, batterTeam, inningAndSide)
        }
        case passedBall(name) => {
          wildPitchPlay(playDescription, name, batterTeam, inningAndSide)
        }
        case interferenceError(name) => {
          val player = runners.playerFromString(name, batterTeam, YearFormatter.print(date))
          val advancements = runners.advancementString(playDescription, player, batterTeam, YearFormatter.print(date))
          val latestPlay = Play(inningAndSide.inning, inningAndSide.side, player.id, "00", "\"" + player.firstName + " " + player.lastName + "\"", "WP" + advancements)
          logger.info("Play: " + latestPlay)
          latestPlay
        }
        case caughtStealing(batterName, runnerName) => {
          val batter = runners.playerFromString(batterName, batterTeam, YearFormatter.print(date))
          val play = atBatPlayString(atBat) + atBatAdvancements(atBat, batter, batterTeam)
          val latestPlay = Play(inningAndSide.inning, inningAndSide.side, batter.id, "00", "", play)
          logger.info("Play: " + latestPlay)
          latestPlay
        }
        case _ => {
          if (!playDescription.startsWith("Pitcher") && !playDescription.startsWith("Batter")) {
            if (!playDescription.startsWith("Pitching ") && !playDescription.contains("Coaching visit to mound.")){
              if (!playDescription.contains("Offensive Substitution: ") && !playDescription.contains("Defensive Substitution") && !playDescription.contains("Defensive Switch") &&
                !playDescription.contains("On-field Delay") && !playDescription.contains("Injury Delay")) {
                logger.warn("Action play was not decoded: " + playDescription)
              }
            }
          }
          null
        }
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

    if (version_2_format) {
      halfInning.findElementsByClassName("table_row") match {
        case atBats: java.util.List[WebElement] => processAtBats(atBats.toList, List.empty[Play]).filter {
          _ != null
        }
        case _ => logger.warn("NO AT BATS FOUND"); List.empty[Play]
      }
    } else {
      halfInning.findElementsByTagName("li") match {
        case atBats: java.util.List[WebElement] => processAtBats(atBats.toList, List.empty[Play]).filter {
          _ != null
        }
        case _ => logger.warn("NO AT BATS FOUND"); List.empty[Play]
      }
    }
  }

  def processedHalfInnings(plays: RemoteWebElement): Map[HalfInning, List[Play]] = {

    def processHalfInning(halfInning: List[WebElement], collection: Map[HalfInning, List[Play]]): Map[HalfInning, List[Play]] = {
      if (halfInning == Nil) {
        logger.warn("HALF INNING EMPTY");
        collection
      } else {
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

    if (version_2_format) {
      plays.findElementsByClassName("inning_tabs") match {
        case x: java.util.List[WebElement] => {
          val innings = x.toList.map(_ match {
            case inning: RemoteWebElement => {
              inning.findElementsByClassName("top").toList ++ inning.findElementsByClassName("bottom").toList
            }
            case _ => null
          })
          processHalfInning(innings.flatMap(x => x), Map.empty[HalfInning, List[Play]])
        }
        case _ => logger.warn("NO HALF INNINGS FOUND"); Map.empty[HalfInning, List[Play]]
      }
    } else {
      plays.findElementsByClassName("plays-half-inning") match {
        case x: java.util.List[WebElement] => processHalfInning(x.toList, Map.empty[HalfInning, List[Play]])
        case _ => logger.warn("NO HALF INNINGS FOUND"); Map.empty[HalfInning, List[Play]]
      }
    }
  }

  def playStrings: List[String] = {
    def eventString(play: Play): String = {
      if (play.play == "sub") {
        "sub," + play.id + "," + play.pitches + "," + play.side + ",0,1\n"
      } else "play," + play.inning + "," + play.side + "," + play.id + "," + play.count + "," + play.pitches + "," + play.play + "\n"
    }

    plays.toList.sortBy(i => (i._1.inning, i._1.side)).foldLeft(List.empty[String])({ (x, y) => x ++ y._2.reverse.map { eventString(_) } }).toList
  }

  if (!(new File(fileName)).exists) {
    val writer = new FileWriter(new File(fileName))
    writer.write(pageSource)
    writer.close
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

  quit
}

