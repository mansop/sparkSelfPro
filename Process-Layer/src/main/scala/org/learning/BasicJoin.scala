package org.learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

class BasicJoin(sc: SparkContext, sqlContext: SQLContext) {

  import sqlContext.implicits._
  private val reader = new LocalFileReader(sc);
  /**
   * Given the CITY and COUNTRY tables, query the sum of the populations of all cities
   * of countries of Contonent
   */
  def sumOfThePopulationsOfAllCitiesByContinent {
    sumOfThePopulationsOfAllCitiesByContinentRDD
    sumOfThePopulationsOfAllCitiesByContinentDF
  }

  def sumOfThePopulationsOfAllCitiesByContinentRDD {
    val cntryPop = reader.readCityFile().map(x => (x(2), x(4).toInt)).reduceByKey(_ + _)
    val cntry = reader.readCountryFile().map(x => (x(0), x(2)))
    val continent = cntryPop.join(cntry)
    val continentWithPop = continent.map(x => (x._2._2, x._2._1)).reduceByKey(_ + _).sortByKey()
    continentWithPop.foreach(println)

  }

  def sumOfThePopulationsOfAllCitiesByContinentDF {

    val splitCity = reader.readCityFile()
    val cityDf = splitCity.map(ln => City(ln(0).toInt, ln(1), ln(2), ln(3), ln(4).toInt)).toDF()

    val splitCountry = reader.readCountryFile()
    val cntryDf = splitCountry.map(ln => Country(ln(0), ln(1), ln(2), ln(3), ln(4).toDouble, ln(5), ln(6).toInt, ln(7), ln(8).toDouble, ln(9), ln(10), ln(11), ln(12), ln(13), ln(14))).toDF()
    //cityDf.join(cntryDf,cityDf("countrycode")===cntryDf("code")).show()
    //cityDf.join(cntryDf,$"countrycode"===$"code").show()
    val cntryPopByCity = cityDf.as("a").join(cntryDf.as("b"), $"a.countrycode" === $"b.code").drop($"b.population")
    cntryPopByCity.groupBy("continent").sum("population").sort("continent").show();

  }

  def getStudentGradeByMarks {
    getStudentGradeByMarksRDD
    getStudentGradeByMarksDF
  }

  def getStudentGradeByMarksRDD {
    val grade = reader.readGradeFile().map(x => (x(0), x(1).toInt, x(2).toInt))
    val student = reader.readStudentFile().map(x => (x(0), x(1), x(2).toInt))
    val joinrdd = student.cartesian(grade)
    val studentGrade = joinrdd.filter(x => x._1._3 >= x._2._2 && x._1._3 <= x._2._3).map(x => (x._1._2, x._2._1, x._1._3)).sortBy(x => x._1.toString())
    studentGrade.foreach(println)
  }

  def getStudentGradeByMarksDF {
    val grade = reader.readGradeFile().map(x => Grade(x(0), x(1).toInt, x(2).toInt)).toDF()
    val student = reader.readStudentFile().map(x => Student(x(0), x(1), x(2).toInt)).toDF()
    student.join(grade, student("mark") >= grade("min_mark") && student("mark") <= grade("max_mark")).select("name", "grade", "mark").orderBy("name").show()
  }
}