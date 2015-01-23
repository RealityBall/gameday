package org.bustos.realityball

import scala.slick.driver.MySQLDriver.simple._

import java.io.File
import scala.io.Source

import RealityballRecords._

object Gameday extends App {

  val db = Database.forURL("jdbc:mysql://localhost:3306/mlbretrosheet", driver="com.mysql.jdbc.Driver", user="root", password="")
  val DataRoot = "/Users/mauricio/Google Drive/Projects/fantasySports/generatedData/"
  val file = new File(DataRoot + "teamMetaData.csv")

  def processSchedules = {
    println("Updating schedules...")
    db.withSession { implicit session =>
      Source.fromFile(file).getLines.foreach { line => 
        if (!line.startsWith("retrosheetId")) {
          val data = line.split(',')
          val schedule = new MlbSchedule(TeamMetaData(data(0), data(1), data(2), data(3), data(4), data(5)), "2015")
          schedule.games.map {println(_)}
        }
      }
    }    
  }
 
  processSchedules
}
