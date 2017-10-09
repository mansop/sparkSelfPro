package org.learning

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit


class BasicSelect(sc: SparkContext, sqlContext: SQLContext) {
  
  import sqlContext.implicits._
  private val reader = new LocalFileReader(sc);

  /**
   * Query all columns for all American cities in CITY with populations larger than 100000.
   *  The CountryCode for America is USA.
   */
  def populationsGrtthnVal(populationVal: Long) {
    populationsGrtthnValRDD(populationVal)
    populationsGrtthnValDF(populationVal)
  }
  def populationsGrtthnValRDD(populationVal: Long) {

    val splitCity = reader.readCityFile()
    val finalRec = splitCity.filter(ln => ln(4).toLong > populationVal && ln(2).equals("USA"))

    finalRec.foreach(rs => println(rs.foreach(fl => print(fl + ","))))

  }
  def populationsGrtthnValDF(populationVal: Long) {

    val splitCity = reader.readCityFile()
    val finalRec = splitCity.map(ln => City(ln(0).toInt, ln(1), ln(2), ln(3), ln(4).toInt))

    val cityDf = finalRec.toDF().registerTempTable("city")
    sqlContext.sql("select * from city where countrycode='USA' and population>" + populationVal).show();

  }

  /**
   * Query a list of CITY names from STATION with even ID numbers only. order City Name,
   * but must exclude duplicates from your answer
   */
  def distictCityName() {
    distictCityNameRDD
    distictCityNameDF
  }
  def distictCityNameRDD() {

    val splitData = reader.readStationFile()
    val city = splitData.filter(ln => (ln(0).toInt % 2) == 0).map(ln => ln(1)).distinct().sortBy(_.toString(), false)
    city.foreach(println)
  }
  def distictCityNameDF() {

    val splitData = reader.readStationFile()
    val station = splitData.map(ln => Station(ln(0).toInt, ln(1), ln(2), ln(3).toDouble, ln(4).toDouble)).toDF()
    station.filter(station("id") % 2 === 0).select("city").distinct().sort($"city".desc).show(300)
  }

  /**
   * Find the difference between the total number of CITY entries in the table
   *  and the number of distinct CITY entries in the table
   *
   */
  def totCityMinusDistictCity() {
    totCityMinusDistictCityRDD
    totCityMinusDistictCityDF
  }
  def totCityMinusDistictCityRDD() {
    val splitData = reader.readStationFile()
    val city = splitData.map(x => x(1))
    val diff = city.count() - city.distinct().count();
    println(diff)
  }
  def totCityMinusDistictCityDF() {
    val splitData = reader.readStationFile()
    val station = splitData.map(ln => Station(ln(0).toInt, ln(1), ln(2), ln(3).toDouble, ln(4).toDouble)).toDF()
    val diff = station.select("city").count() - station.select("city").distinct().count()
    println(diff)
  }

  /**
   * Query the two cities in STATION with the shortest and longest CITY names,
   * as well as their respective lengths (i.e.: number of characters in the name).
   * If there is more than one smallest or largest city,
   * choose the one that comes first when ordered alphabetically.
   *
   */
  def queryTwoCity() {
    queryTwoCityRDD
    queryTwoCityDF
  }
  def queryTwoCityRDD() {
    val splitData = reader.readStationFile()
    val cityWithLen = splitData.map(x => (x(1), x(1).length()))
    val max = cityWithLen.map(_._2).max()
    val min = cityWithLen.map(_._2).min()
    val res1 = cityWithLen.filter(x => x._2 == min).sortByKey(true).take(1)
    val res2 = cityWithLen.filter(x => x._2 == max).sortByKey(true).take(1)
    val res = res1.union(res2)
    res.foreach(println)
  }
  def queryTwoCityDF() {
    val splitData = reader.readStationFile()
    val station = splitData.map(ln => Station(ln(0).toInt, ln(1), ln(2), ln(3).toDouble, ln(4).toDouble)).toDF().registerTempTable("station")
    var sql = "select * from ("
    sql += "select city,(length(trim(city))) from station"
    sql += " order by (length(trim(city))) ,city limit 1) A"
    sql += " UNION ALL "
    sql += "select * from ("
    sql += "select city,(length(trim(city))) from station"
    sql += " order by (length(trim(city))) desc,city limit 1) B"
    //println(sql)
    sqlContext.sql(sql).show();
  }

  /**
   * Query the list of CITY names starting & Ending with vowels (i.e., a, e, i, o, or u) from STATION.
   * Your result cannot contain duplicates.
   */
  def cityNameWithVowels() {
    cityNameWithVowelsRDD
    cityNameWithVowelsDF
  }
  def cityNameWithVowelsRDD() {
    val splitData = reader.readStationFile().map(x => x(1))
    val city = splitData.filter(x => x.substring(0, 1).equals("A") || x.substring(0, 1).equals("E")
      || x.substring(0, 1).equals("I") || x.substring(0, 1).equals("O")
      || x.substring(0, 1).equals("U")).distinct().sortBy(x => x.toString());
    city.foreach(println)
    println()
  }
  def cityNameWithVowelsDF() {

    val splitData = reader.readStationFile()
    val station = splitData.map(ln => (ln(0).toInt, ln(1), ln(2), ln(3).toDouble, ln(4).toDouble)).toDF("id", "city", "state", "lat_n", "long_w")
    //station.filter(station("name").substr(0, 1)).show();
    station.select("city","state").where("city like 'A%' or city like 'E%' or city like 'I%' or city like 'O%' or city like 'U%'").sort($"city".desc).show(1000);
    station.select("city","state").filter("city like '%a' or city like '%e' or city like '%i' or city like '%o' or city like '%o'").sort($"city".desc).show(1000);
    station.select("city","state").withColumn("constant", lit(8)).show();
  }
  
}