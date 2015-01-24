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
  
  Thread sleep 5
  
  var pitchCount = 0
  
  find("plays") match {
    case Some(x) => x.underlying match {
      case plays: RemoteWebElement => {
        plays.findElementsByClassName("plays-half-inning") match {
          case halfInnings: java.util.List[WebElement] => {
            halfInnings.toList.foreach { _ match {
              case halfInning: RemoteWebElement => {
                halfInning.findElementsByClassName("plays-atbat") match {
                  case atBats: java.util.List[WebElement] => {
                    atBats.toList.foreach { _ match {
                      case atBat: RemoteWebElement => {
                        println(atBat.getText)
                        atBat.findElementsByClassName("plays-pitch-result") match {
                          case pitches: java.util.List[WebElement] => {
                            pitches.toList.foreach { _ match {
                                case pitch: RemoteWebElement => {
                                  pitchCount = pitchCount + 1
                                  println(pitchCount)
                                  println(pitch.getAttribute("textContent")) 
                                }
                                case _ =>
                              }
                            }
                          }
                          case _ =>
                        }
                      }
                      case _ =>
                      }
                    }
                  }
                  case _ =>
                }
              }
              case _ =>
              }
            }
          }
          case _ =>
        }
      }
      case _ =>       
    } 
    case _ =>
  }
  quit
}