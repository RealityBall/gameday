package org.bustos.realityball

import scala.slick.driver.MySQLDriver.simple._
import scala.util.Properties.envOrElse

object RealityballConfig {
  
  val GamedayURL = "http://mlb.com/mlb/gameday/"
  val DataRoot = "/Users/mauricio/Google Drive/Projects/fantasySports/generatedData/"
  
  val WUNDERGROUND_APIURL = "http://api.wunderground.com/api/" 
  val WUNDERGROUND_APIKEY = "eeb51f60b8bd49aa"
  
  val db = {
    val mysqlURL = envOrElse("MLB_MYSQL_URL", "jdbc:mysql://localhost:3306/mlbretrosheet")
    val mysqlUser = envOrElse("MLB_MYSQL_USER", "root")
    val mysqlPassword = envOrElse("MLB_MYSQL_PASSWORD", "")
    Database.forURL(mysqlURL, driver="com.mysql.jdbc.Driver", user=mysqlUser, password=mysqlPassword)
  }
      
}