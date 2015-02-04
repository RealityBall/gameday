package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
import scala.slick.driver.MySQLDriver.simple._
import scala.util.Properties.envOrNone
import spray.json._
import DefaultJsonProtocol._
import scala.slick.jdbc.{ GetResult, StaticQuery => Q }
import RealityballConfig._

class RealityballData {

  import RealityballRecords._
  import GoogleTableJsonProtocol._

  def teams(year: String): List[Team] = {
    db.withSession { implicit session =>
      if (year == "All" || year.toInt > 2014) {
        teamsTable.filter(_.year === "2014").sortBy(_.mnemonic).list
      } else {
        teamsTable.filter(_.year === year).sortBy(_.mnemonic).list
      }
    }
  }

  def playerFromRetrosheetId(retrosheetId: String, year: String): Player = {
    db.withSession { implicit session =>
      val playerList = {
        if (year == "") {
          val playerList = playersTable.filter({ x => x.id === retrosheetId }).list
          if (playerList.isEmpty) throw new IllegalStateException("No one found with Retrosheet ID: " + retrosheetId)
          else playerList
        } else {
          val playerList = playersTable.filter({ x => x.id === retrosheetId && x.year === year }).list
          if (playerList.isEmpty) throw new IllegalStateException("No one found with Retrosheet ID: " + retrosheetId + " in year " + year)
          else if (playerList.length > 1) throw new IllegalStateException("Non Unique Retrosheet ID: " + retrosheetId + " in year " + year)
          else playerList
        }
      }
      playerList.head
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

  def mappingForRetroId(id: String): IdMapping = {
    db.withSession { implicit session =>
      val mappingList = idMappingTable.filter({ x => x.retroId === id }).list
      if (mappingList.length == 1) mappingList.head
      else IdMapping("", "", "", "", "", "", "", "", "")
    }
  }

  def batterSummary(id: String, year: String): PlayerData = {
    db.withSession { implicit session =>
      val playerMnemonic = truePlayerID(id)
      val mapping = mappingForRetroId(playerMnemonic)
      if (year == "All") {
        val player = playersTable.filter(_.id === playerMnemonic).list.head
        val RHatBats = hitterRawRH.filter(_.id === playerMnemonic).map(_.RHatBat).sum.run.get
        val LHatBats = hitterRawLH.filter(_.id === playerMnemonic).map(_.LHatBat).sum.run.get
        val gamesQuery = for {
          r <- hitterRawLH if r.id === playerMnemonic;
          l <- hitterRawRH if (l.id === playerMnemonic && l.date === r.date)
        } yield (1)
        val summary = PlayerSummary(playerMnemonic, RHatBats, LHatBats, gamesQuery.length.run, mapping.mlbId, mapping.brefId, mapping.espnId)
        PlayerData(player, summary)
      } else {
        val player = playersTable.filter({ x => x.id === playerMnemonic && x.year.startsWith(year) }).list.head
        val RHatBats = hitterRawRH.filter({ x => x.id === playerMnemonic && x.date.startsWith(year) }).map(_.RHatBat).sum.run.get
        val LHatBats = hitterRawLH.filter({ x => x.id === playerMnemonic && x.date.startsWith(year) }).map(_.LHatBat).sum.run.get
        val gamesQuery = for {
          r <- hitterRawLH if r.id === playerMnemonic && r.date.startsWith(year);
          l <- hitterRawRH if (l.id === playerMnemonic && l.date === r.date)
        } yield (1)
        val summary = PlayerSummary(playerMnemonic, RHatBats, LHatBats, gamesQuery.length.run, mapping.mlbId, mapping.brefId, mapping.espnId)
        PlayerData(player, summary)
      }
    }
  }

  def pitcherSummary(id: String, year: String): PitcherData = {
    db.withSession { implicit session =>
      val playerMnemonic = truePlayerID(id)
      val mapping = mappingForRetroId(playerMnemonic)
      if (year == "All") {
        val player = playersTable.filter(_.id === playerMnemonic).list.head
        val win = pitcherStats.filter(x => x.id === playerMnemonic && x.win === 1).length.run
        val loss = pitcherStats.filter(x => x.id === playerMnemonic && x.loss === 1).length.run
        val save = pitcherStats.filter(x => x.id === playerMnemonic && x.save === 1).length.run
        val games = pitcherStats.filter(x => x.id === playerMnemonic).length.run
        val summary = PitcherSummary(playerMnemonic, win, loss, save, games, mapping.mlbId, mapping.brefId, mapping.espnId)
        PitcherData(player, summary)
      } else {
        val player = playersTable.filter(x => x.id === playerMnemonic && x.year.startsWith(year)).list.head
        val win = pitcherStats.filter(x => x.id === playerMnemonic && x.win === 1 && x.date.startsWith(year)).length.run
        val loss = pitcherStats.filter(x => x.id === playerMnemonic && x.loss === 1 && x.date.startsWith(year)).length.run
        val save = pitcherStats.filter(x => x.id === playerMnemonic && x.save === 1 && x.date.startsWith(year)).length.run
        val games = pitcherStats.filter(x => x.id === playerMnemonic && x.date.startsWith(year)).length.run
        val summary = PitcherSummary(playerMnemonic, win, loss, save, games, mapping.mlbId, mapping.brefId, mapping.espnId)
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

  def dataNumericTable(data: List[(String, AnyVal)], title: String): String = {
    val columns = List(new GoogleColumn("Date", "Date", "string"), new GoogleColumn(title, title, "number"))
    val rows = data.map(obs => GoogleRow(List(new GoogleCell(obs._1), new GoogleCell(obs._2))))
    GoogleTable(columns, rows).toJson.prettyPrint
  }

  def dataNumericTable2(data: List[(String, AnyVal, AnyVal)], titles: List[String]): String = {
    val columns = List(new GoogleColumn("Date", "Date", "string"), new GoogleColumn(titles(0), titles(0), "number"), new GoogleColumn(titles(1), titles(1), "number"))
    val rows = data.map(obs => GoogleRow(List(new GoogleCell(obs._1), new GoogleCell(obs._2), new GoogleCell(obs._3))))
    GoogleTable(columns, rows).toJson.prettyPrint
  }

  def dataNumericTable3(data: List[(String, AnyVal, AnyVal, AnyVal)], titles: List[String]): String = {
    val columns = List(new GoogleColumn("Date", "Date", "string"), new GoogleColumn(titles(0), titles(0), "number"), new GoogleColumn(titles(1), titles(1), "number"), new GoogleColumn(titles(2), titles(2), "number"))
    val rows = data.map(obs => GoogleRow(List(new GoogleCell(obs._1), new GoogleCell(obs._2), new GoogleCell(obs._3), new GoogleCell(obs._4))))
    GoogleTable(columns, rows).toJson.prettyPrint
  }

  def dataNumericPieChart(data: List[(String, AnyVal)], title: String, units: String): String = {
    val columns = List(new GoogleColumn(title, title, "string"), new GoogleColumn(units, units, "string"))
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

  def outs(id: String, year: String): List[(String, Double, Double, Double)] = {
    db.withSession { implicit session =>
      pitcherDailyQuery(id, year).map(p => (p.date, p.strikeOuts, p.flyOuts, p.groundOuts)).list.map({ x => (x._1, x._2.toDouble.max(0.001), x._3.toDouble.max(0.001), x._4.toDouble.max(0.001)) })
    }
  }

  def outsTypeCount(id: String, year: String): List[(String, Double)] = {
    db.withSession { implicit session =>
      if (year == "All") {
        val q = Q[String, (Double, Double, Double)] + """
              select sum(strikeOuts) as souts, sum(flyOuts) as fouts, sum(groudOuts) as gouts
              from
                pitcherDaily
              where
                id = ?
            """
        val result = q(id).first
        List(("Strikeouts", result._1), ("Flyouts", result._2), ("Groundouts", result._3))
      } else {
        val q = Q[(String, String), (Double, Double, Double)] + """
              select sum(strikeOuts) as souts, sum(flyOuts) as fouts, sum(groundOuts) as gouts
              from
                pitcherDaily
              where
                id = ? and instr(date, ?) > 0
              """
        val result = q(id, year).first
        List(("Strikeouts", result._1), ("Flyouts", result._2), ("Groundouts", result._3))
      }
    }
  }

  def strikeRatio(id: String, year: String): List[(String, Double)] = {
    db.withSession { implicit session =>
      pitcherDailyQuery(id, year).map(p => (p.date, p.pitches, p.balls)).list.map({ x => (x._1, (x._2 - x._3).toDouble / x._2.toDouble) })
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

  def teamFantasy(team: String, year: String): List[(String, Double, Double)] = {
    import scala.collection.mutable.Queue

    def withMovingAverage(list: List[(String, Double)]): List[(String, Double, Double)] = {
      var running = Queue.empty[Double]
      list.map({ x =>
        {
          running.enqueue(x._2)
          if (running.size > 25) running.dequeue
          (x._1, x._2, running.foldLeft(0.0)(_ + _) / running.size)
        }
      })
    }

    db.withSession { implicit session =>
      if (year == "All") {
        val q = Q[(String, String), (String, Double)] + """
              select date, sum(fanDuel) from
              (select * from hitterFantasyStats where side = 0 and gameId in (select id from games where visitingTeam = ?)
              union
              select * from hitterFantasyStats where side = 1 and gameId in (select id from games where homeTeam = ?)) history
              group by date order by date
            """
        withMovingAverage(q(team, team).list)
      } else {
        val q = Q[(String, String, String, String), (String, Double)] + """
              select date, sum(fanDuel) from
              (select * from hitterFantasyStats where side = 0 and gameId in (select id from games where visitingTeam = ? and instr(date, ?) > 0)
              union
              select * from hitterFantasyStats where side = 1 and gameId in (select id from games where homeTeam = ? and instr(date, ?) > 0)) history
              group by date order by date
            """
        withMovingAverage(q(team, year, team, year).list)
      }
    }
  }

  def ballparkBA(team: String, year: String): List[BattingAverageObservation] = {
    import scala.collection.mutable.Queue

    def safeRatio(x: Double, y: Double): Double = {
      if (y != 0.0) x / y
      else Double.NaN
    }

    def replaceWithMovingAverage(list: List[BattingAverageObservation]): List[BattingAverageObservation] = {
      var running_1 = Queue.empty[Double]
      var running_2 = Queue.empty[Double]
      var running_3 = Queue.empty[Double]

      list.map({ x =>
        {
          if (!x.bAvg.isNaN) running_1.enqueue(x.bAvg)
          if (running_1.size > 25) running_1.dequeue
          if (!x.lhBAvg.isNaN) running_2.enqueue(x.lhBAvg)
          if (running_2.size > 25) running_3.dequeue
          if (!x.rhBAvg.isNaN) running_3.enqueue(x.rhBAvg)
          if (running_3.size > 25) running_3.dequeue
          BattingAverageObservation(x.date, running_1.foldLeft(0.0)(_ + _) / running_1.size, running_2.foldLeft(0.0)(_ + _) / running_2.size, running_3.foldLeft(0.0)(_ + _) / running_3.size)
        }
      })
    }

    db.withSession { implicit session =>
      if (year == "All") replaceWithMovingAverage(ballparkDailiesTable.filter(_.id like team + "%").sortBy(_.id).list.map({ x =>
        BattingAverageObservation(x.date, safeRatio((x.LHhits + x.RHhits), (x.LHatBat + x.RHatBat)), safeRatio(x.LHhits, x.LHatBat), safeRatio(x.RHhits, x.RHatBat))
      }))
      else replaceWithMovingAverage(ballparkDailiesTable.filter({ row => (row.id like (team + "%")) && (row.id like (team + year + "%")) }).list.map({ x =>
        BattingAverageObservation(x.date, safeRatio((x.LHhits + x.RHhits), (x.LHatBat + x.RHatBat)), safeRatio(x.LHhits, x.LHatBat), safeRatio(x.RHhits, x.RHatBat))
      }))
    }
  }

  def ballparkAttendance(team: String, year: String): List[(String, Double)] = {
    def dateFromId(id: String): String = {
      id.substring(3, 7) + "/" + id.substring(7, 9) + "/" + id.substring(9, 11)
    }
    db.withSession { implicit session =>
      if (year == "All") gameScoringTable.filter(_.id like team + "%").sortBy(_.id).list.map({ x => (dateFromId(x.id), x.attendance.toDouble) })
      else gameScoringTable.filter({ row => (row.id like (team + "%")) && (row.id like (team + year + "%")) }).list.map({ x => (dateFromId(x.id), x.attendance.toDouble) })
    }
  }

  def ballparkTemp(team: String, year: String): List[(String, Double)] = {
    db.withSession { implicit session =>
      val teamMeta = teamsTable.filter({ x => x.year === "2014" && x.mnemonic === team }).list
      if (teamMeta.isEmpty) List.empty[(String, Double)]
      else {
        val weather = new Weather(teamMeta.head.zipCode)
        val formatter = DateTimeFormat.forPattern("MM/dd H:mm a")
        weather.hourlyForecasts.map({ x =>
          {
            val date = new DateTime(x.FCTTIME.epoch.toLong * 1000)
            (formatter.print(date), x.temp.english.toDouble)
          }
        })
      }
    }
  }

  def schedule(team: String, year: String): List[FullGameInfo] = {
    db.withSession { implicit session =>
      val formatter = DateTimeFormat.forPattern("yyyyMMdd")
      val lookingOut = formatter.print((new DateTime).plusMonths(3))
      val schedule = {
        if (year == "All") gamedayScheduleTable.filter({ x => ((x.homeTeam === team) || (x.visitingTeam === team)) && x.date < lookingOut }).sortBy(_.date).list
        else gamedayScheduleTable.filter({ x => ((x.homeTeam === team) || (x.visitingTeam === team)) && x.date < lookingOut }).sortBy(_.date).list
      }
      schedule.reverse.map { x =>
        val oddsForGame = gameOddsTable.filter({ y => y.id === x.id }).list
        if (oddsForGame.isEmpty) FullGameInfo(x, GameOdds("", 0, 0, 0.0, 0))
        else FullGameInfo(x, oddsForGame.head)
      }
    }
  }

  def injuries(team: String): List[InjuryReport] = {
    db.withSession { implicit session =>
      val injuryReports = for {
        injuries <- injuryReportTable
        ids <- idMappingTable if injuries.mlbId === ids.mlbId
      } yield (ids.mlbName, injuries.reportTime, injuries.injuryReportDate, injuries.status, injuries.dueBack, injuries.injury)
      injuryReports.list.map({ x => InjuryReport(x._1, x._2, x._3, x._4, x._5, x._6) })
    }
  }

}
