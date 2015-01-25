package org.bustos.realityball

import java.util.Date
import org.scalatest._
import selenium._
import org.scalatest.time.{Span, Seconds}
import org.openqa.selenium.support.ui.{WebDriverWait, ExpectedCondition}
import org.openqa.selenium._
import remote._
import htmlunit._
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import RealityballRecords._
import RealityballConfig._

class MlbPlays(date: Date, awayTeam: String, homeTeam: String) extends Chrome {

  implicitlyWait(Span(20, Seconds))
  
  val logger =  LoggerFactory.getLogger(getClass)
  logger.info ("********************************")
  logger.info ("*** Retrieving play results for " + awayTeam + " @ " + homeTeam)
  logger.info ("********************************")
  
  val host = GamedayURL
  go to host + "index.jsp?gid=2014_04_22_miamlb_atlmlb_1&mode=plays"
  
  var pitchCount = 0

  def processPitch(pitch: RemoteWebElement) = {
    if (pitch.getTagName == "td") {
      pitchCount = pitchCount + 1
      logger.info(pitchCount + " " + pitch.getAttribute("textContent"))
    }
  }
  
  def processAtBat(atBat: RemoteWebElement) = {
    atBat.findElementByClassName("plays-atbat-pitcher") match {
      case pitcher: RemoteWebElement => logger.info(pitcher.getAttribute("textContent"))
      case _ => logger.info("NO PITCHER FOUND")
    }
    atBat.findElementByClassName("plays-atbat-batter") match {
      case batter: RemoteWebElement => logger.info(batter.getAttribute("textContent"))
      case _ => logger.info("NO BATTER FOUND")
    }
    atBat.findElementsByClassName("plays-pitch-result") match {
      case pitches: java.util.List[WebElement] => {
        pitches.toList.foreach { _ match {
            case pitch: RemoteWebElement => processPitch(pitch)
            case _ => logger.info("INVALID PITCH")
          }
        }
      }
      case _ => logger.info("NO PITCHES FOUND")
    }
  }
  
  def processHalfInning(halfInning: RemoteWebElement) = {
    halfInning.findElementsByClassName("plays-atbat") match {
      case atBats: java.util.List[WebElement] => {
        atBats.toList.foreach { _ match {
            case atBat: RemoteWebElement => processAtBat(atBat)
            case _ => logger.info("INVALID AT BAT")
          }
        }
      }
      case _ => logger.info("NO AT BATS FOUND")
    }
  }
  
  def processHalfInnings(plays: RemoteWebElement) = {
    plays.findElementsByClassName("plays-half-inning") match {
      case halfInnings: java.util.List[WebElement] => {
        halfInnings.toList.foreach { _ match {
            case halfInning: RemoteWebElement => processHalfInning(halfInning)
            case _ => logger.info("INVALID HALF INNING")
          }
        }
      }
      case _ => logger.info("NO HALF INNINGS FOUND")
    }
  }
  
  find("plays") match {
    case Some(x) => x.underlying match {
      case plays: RemoteWebElement => processHalfInnings(plays)
      case _ =>       
    } 
    case _ => logger.info("NO PLAYS FOUND")
  }
  quit
}