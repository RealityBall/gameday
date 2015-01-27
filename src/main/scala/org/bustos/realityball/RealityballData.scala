package org.bustos.realityball

import scala.slick.driver.MySQLDriver.simple._
import scala.util.Properties.envOrNone
import spray.json._
import DefaultJsonProtocol._
import scala.slick.jdbc.{ GetResult, StaticQuery => Q }
import RealityballConfig._

class RealityballData {

  import RealityballRecords._
  import GoogleTableJsonProtocol._

  def teams(year: String): List[(String, String, String, String, String)] = {
    db.withSession { implicit session =>
      if (year == "All") {
        Q.queryNA[(String, String, String, String, String)]("select distinct '2014', mnemonic, league, city, name from teams order by mnemonic").list
      } else {
        teamsTable.filter(_.year === year).sortBy(_.mnemonic).list.map({ x => (x.year, x.mnemonic, x.league, x.city, x.name) })
      }
    }
  }

  def playerFromRetrosheetId(retrosheetId: String, year: String): Player = {
    db.withSession { implicit session =>
      val playerList = playersTable.filter({ x => x.id === retrosheetId && x.year === year }).list
      if (playerList.isEmpty) throw new IllegalStateException("No one found with Retrosheet ID: " + retrosheetId)
      else if (playerList.length > 1) throw new IllegalStateException("Non Unique Retrosheet ID: " + retrosheetId)
      else playerList.head
    }
  }

  def playerFromMlbId(mlbId: String, year: String): Player = {
    db.withSession { implicit session =>
      val mappingList = idMappingTable.filter({ x => x.mlbId === mlbId }).list
      if (mappingList.isEmpty) throw new IllegalStateException("No one found with MLB ID: " + mlbId)
      else if (mappingList.length > 1) throw new IllegalStateException("Non Unique MLB ID: " + mlbId)
      playerFromRetrosheetId(mappingList.head.retroId, year)
    }
  }

  def playerFromName(firstName: String, lastName: String, year: String): Player = {
    db.withSession { implicit session =>
      val playerList = playersTable.filter({ x => x.firstName.like(firstName + "%") && x.lastName === lastName && x.year === year }).list
      if (playerList.isEmpty) throw new IllegalStateException("No one found by the name of: " + firstName + " " + lastName)
      else if (playerList.length > 1) throw new IllegalStateException("Non Unique Name: " + firstName + " " + lastName)
      else playerList.head
    }
  }

  def players(team: String, year: String): List[Player] = {
    db.withSession { implicit session =>
      if (year == "All") {
        val groups = playersTable.filter(_.team === team).list.groupBy(_.id)
        val players = groups.mapValues(_.head).values
        val partitions = players.partition(_.position != "P")
        partitions._1.toList.sortBy(_.lastName) ++ partitions._2.toList.sortBy(_.lastName)
      } else {
        val partitions = playersTable.filter({ x => x.team === team && x.year === year }).list.partition(_.position != "P")
        partitions._1.sortBy(_.lastName) ++ partitions._2.sortBy(_.lastName)
      }
    }
  }

  def batterSummary(id: String, year: String): PlayerData = {
    db.withSession { implicit session =>
      val playerMnemonic = truePlayerID(id)
      if (year == "All") {
        val player = playersTable.filter(_.id === playerMnemonic).list.head
        val RHatBats = hitterRawRH.filter(_.id === playerMnemonic).map(_.RHatBat).sum.run.get
        val LHatBats = hitterRawLH.filter(_.id === playerMnemonic).map(_.LHatBat).sum.run.get
        val gamesQuery = for {
          r <- hitterRawLH if r.id === playerMnemonic;
          l <- hitterRawRH if (l.id === playerMnemonic && l.date === r.date)
        } yield (1)
        val summary = PlayerSummary(playerMnemonic, RHatBats, LHatBats, gamesQuery.length.run)
        PlayerData(player, summary)
      } else {
        val player = playersTable.filter({ x => x.id === playerMnemonic && x.year.startsWith(year) }).list.head
        val RHatBats = hitterRawRH.filter({ x => x.id === playerMnemonic && x.date.startsWith(year) }).map(_.RHatBat).sum.run.get
        val LHatBats = hitterRawLH.filter({ x => x.id === playerMnemonic && x.date.startsWith(year) }).map(_.LHatBat).sum.run.get
        val gamesQuery = for {
          r <- hitterRawLH if r.id === playerMnemonic && r.date.startsWith(year);
          l <- hitterRawRH if (l.id === playerMnemonic && l.date === r.date)
        } yield (1)
        val summary = PlayerSummary(playerMnemonic, RHatBats, LHatBats, gamesQuery.length.run)
        PlayerData(player, summary)
      }
    }
  }

  def pitcherSummary(id: String, year: String): PitcherData = {
    db.withSession { implicit session =>
      val playerMnemonic = truePlayerID(id)
      if (year == "All") {
        val player = playersTable.filter(_.id === playerMnemonic).list.head
        val win = pitcherStats.filter(x => x.id === playerMnemonic && x.win === 1).length.run
        val loss = pitcherStats.filter(x => x.id === playerMnemonic && x.loss === 1).length.run
        val save = pitcherStats.filter(x => x.id === playerMnemonic && x.save === 1).length.run
        val games = pitcherStats.filter(x => x.id === playerMnemonic).length.run
        val summary = PitcherSummary(playerMnemonic, win, loss, save, games)
        PitcherData(player, summary)
      } else {
        val player = playersTable.filter(x => x.id === playerMnemonic && x.year.startsWith(year)).list.head
        val win = pitcherStats.filter(x => x.id === playerMnemonic && x.win === 1 && x.date.startsWith(year)).length.run
        val loss = pitcherStats.filter(x => x.id === playerMnemonic && x.loss === 1 && x.date.startsWith(year)).length.run
        val save = pitcherStats.filter(x => x.id === playerMnemonic && x.save === 1 && x.date.startsWith(year)).length.run
        val games = pitcherStats.filter(x => x.id === playerMnemonic && x.date.startsWith(year)).length.run
        val summary = PitcherSummary(playerMnemonic, win, loss, save, games)
        PitcherData(player, summary)
      }
    }
  }

  def truePlayerID(id: String): String = {
    if (!id.contains("[")) {
      id
    } else {
      id.split("[")(1).replaceAll("]", "")
    }
  }

  def dataTable(data: List[BattingAverageObservation]): String = {
    val columns = List(new GoogleColumn("Date", "Date", "string"), new GoogleColumn("Total", "Total", "number"), new GoogleColumn("Lefties", "Against Lefties", "number"), new GoogleColumn("Righties", "Against Righties", "number"))
    val rows = data.map(ba => GoogleRow(List(new GoogleCell(ba.date), new GoogleCell(ba.bAvg), new GoogleCell(ba.lhBAvg), new GoogleCell(ba.rhBAvg))))
    GoogleTable(columns, rows).toJson.prettyPrint
  }

  def dataNumericTable(data: List[(String, AnyVal)]): String = {
    val columns = List(new GoogleColumn("Date", "Date", "string"), new GoogleColumn("Total", "Total", "number"))
    val rows = data.map(obs => GoogleRow(List(new GoogleCell(obs._1), new GoogleCell(obs._2))))
    GoogleTable(columns, rows).toJson.prettyPrint
  }

  def displayDouble(x: Option[Double]): Double = {
    x match {
      case None => Double.NaN
      case _    => x.get
    }
  }

  def years: List[String] = {
    db.withSession { implicit session =>
      "All" :: (Q.queryNA[String]("select distinct(substring(date, 1, 4)) as year from games order by year").list)
    }
  }

  def hitterStatsQuery(id: String, year: String): Query[HitterDailyStatsTable, HitterDailyStatsTable#TableElementType, Seq] = {
    if (year == "All") hitterStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else hitterStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def hitterMovingStatsQuery(id: String, year: String): Query[HitterStatsMovingTable, HitterStatsMovingTable#TableElementType, Seq] = {
    if (year == "All") hitterMovingStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else hitterMovingStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def hitterFantasyQuery(id: String, year: String): Query[HitterFantasyTable, HitterFantasyTable#TableElementType, Seq] = {
    if (year == "All") hitterFantasyStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else hitterFantasyStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def hitterFantasyMovingQuery(id: String, year: String): Query[HitterFantasyMovingTable, HitterFantasyMovingTable#TableElementType, Seq] = {
    if (year == "All") hitterFantasyMovingStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else hitterFantasyMovingStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def pitcherFantasyQuery(id: String, year: String): Query[PitcherFantasyTable, PitcherFantasyTable#TableElementType, Seq] = {
    if (year == "All") pitcherFantasyStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else pitcherFantasyStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def pitcherFantasyMovingQuery(id: String, year: String): Query[PitcherFantasyMovingTable, PitcherFantasyMovingTable#TableElementType, Seq] = {
    if (year == "All") pitcherFantasyMovingStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else pitcherFantasyMovingStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def hitterVolatilityStatsQuery(id: String, year: String): Query[HitterStatsVolatilityTable, HitterStatsVolatilityTable#TableElementType, Seq] = {
    if (year == "All") hitterVolatilityStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else hitterVolatilityStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def pitcherDailyQuery(id: String, year: String): Query[PitcherDailyTable, PitcherDailyTable#TableElementType, Seq] = {
    if (year == "All") pitcherStats.filter(_.id === truePlayerID(id)).sortBy(_.date)
    else pitcherStats.filter({ x => x.id === truePlayerID(id) && x.date.startsWith(year) }).sortBy(_.date)
  }

  def BA(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterStatsQuery(id, year).map(p => (p.date, p.battingAverage, p.LHbattingAverage, p.RHbattingAverage)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def movingBA(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterMovingStatsQuery(id, year).map(p => (p.date, p.battingAverageMov, p.LHbattingAverageMov, p.RHbattingAverageMov)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def volatilityBA(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterVolatilityStatsQuery(id, year).map(p => (p.date, p.battingVolatility, p.LHbattingVolatility, p.RHbattingVolatility)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def dailyBA(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterStatsQuery(id, year).map(p => (p.date, p.dailyBattingAverage, p.LHdailyBattingAverage, p.RHdailyBattingAverage)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def fantasy(id: String, year: String, gameName: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = {
        gameName match {
          case "FanDuel"    => hitterFantasyQuery(id, year).map(p => (p.date, p.fanDuel, p.LHfanDuel, p.RHfanDuel)).list
          case "DraftKings" => hitterFantasyQuery(id, year).map(p => (p.date, p.draftKings, p.LHdraftKings, p.RHdraftKings)).list
          case "Draftster"  => hitterFantasyQuery(id, year).map(p => (p.date, p.draftster, p.LHdraftster, p.RHdraftster)).list
        }
      }
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def fantasyMoving(id: String, year: String, gameName: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = {
        gameName match {
          case "FanDuel"    => hitterFantasyMovingQuery(id, year).map(p => (p.date, p.fanDuelMov, p.LHfanDuelMov, p.RHfanDuelMov)).list
          case "DraftKings" => hitterFantasyMovingQuery(id, year).map(p => (p.date, p.draftKingsMov, p.LHdraftKingsMov, p.RHdraftKingsMov)).list
          case "Draftster"  => hitterFantasyMovingQuery(id, year).map(p => (p.date, p.draftsterMov, p.LHdraftsterMov, p.RHdraftsterMov)).list
        }
      }
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def slugging(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterStatsQuery(id, year).map(p => (p.date, p.sluggingPercentage, p.LHsluggingPercentage, p.RHsluggingPercentage)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def onBase(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterStatsQuery(id, year).map(p => (p.date, p.onBasePercentage, p.LHonBasePercentage, p.RHonBasePercentage)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def sluggingMoving(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterMovingStatsQuery(id, year).map(p => (p.date, p.sluggingPercentageMov, p.LHsluggingPercentageMov, p.RHsluggingPercentageMov)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def onBaseMoving(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterMovingStatsQuery(id, year).map(p => (p.date, p.onBasePercentageMov, p.LHonBasePercentageMov, p.RHonBasePercentageMov)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def sluggingVolatility(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterVolatilityStatsQuery(id, year).sortBy(_.date).map(p => (p.date, p.sluggingVolatility, p.LHsluggingVolatility, p.RHsluggingVolatility)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def onBaseVolatility(id: String, year: String): List[BattingAverageObservation] = {
    db.withSession { implicit session =>
      val playerStats = hitterVolatilityStatsQuery(id, year).sortBy(_.date).map(p => (p.date, p.onBaseVolatility, p.LHonBaseVolatility, p.RHonBaseVolatility)).list
      playerStats.map({ x => BattingAverageObservation(x._1, displayDouble(x._2), displayDouble(x._3), displayDouble(x._4)) })
    }
  }

  def outs(id: String, year: String): List[(String, Int)] = {
    db.withSession { implicit session =>
      pitcherDailyQuery(id, year).sortBy(_.date).map(p => (p.date, p.outs)).list.map({ x => (x._1, x._2) })
    }
  }

  def strikeRatio(id: String, year: String): List[(String, Double)] = {
    db.withSession { implicit session =>
      pitcherDailyQuery(id, year).sortBy(_.date).map(p => (p.date, p.pitches, p.balls)).list.map({ x => (x._1, (x._2 - x._3).toDouble / x._2.toDouble) })
    }
  }

  def pitcherFantasy(id: String, year: String, gameName: String): List[(String, Double)] = {
    db.withSession { implicit session =>
      val playerStats = {
        gameName match {
          case "FanDuel"    => pitcherFantasyQuery(id, year).map(p => (p.date, p.fanDuel)).list
          case "DraftKings" => pitcherFantasyQuery(id, year).map(p => (p.date, p.draftKings)).list
          case "Draftster"  => pitcherFantasyQuery(id, year).map(p => (p.date, p.draftster)).list
        }
      }
      playerStats.map({ x => (x._1, displayDouble(x._2)) })
    }
  }

  def pitcherFantasyMoving(id: String, year: String, gameName: String): List[(String, Double)] = {
    db.withSession { implicit session =>
      val playerStats = {
        gameName match {
          case "FanDuel"    => pitcherFantasyMovingQuery(id, year).map(p => (p.date, p.fanDuelMov)).list
          case "DraftKings" => pitcherFantasyMovingQuery(id, year).map(p => (p.date, p.draftKingsMov)).list
          case "Draftster"  => pitcherFantasyMovingQuery(id, year).map(p => (p.date, p.draftsterMov)).list
        }
      }
      playerStats.map({ x => (x._1, displayDouble(x._2)) })
    }
  }
}
