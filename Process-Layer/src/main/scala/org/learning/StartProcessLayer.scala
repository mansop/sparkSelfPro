package org.learning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

class StartProcessLayer {

  private val conf = new SparkConf().setMaster("local").setAppName("My App");
  
  //private val conf = new SparkConf().setAppName("My App");
  private val sc = new SparkContext(conf);
  private val sqlContext= new SQLContext(sc)
  

  def executeBasicSelect(args: Array[String]) {

    val basicSelect = new BasicSelect(sc,sqlContext)
    
    //basicSelect.populationsGrtthnVal(100000)
    //basicSelect.distictCityName()
    //basicSelect.totCityMinusDistictCity()
    //basicSelect.queryTwoCity()
    basicSelect.cityNameWithVowels()
    
    val basicjoin = new BasicJoin(sc,sqlContext)
    //basicjoin.sumOfThePopulationsOfAllCitiesByContinent
    //basicjoin.getStudentGradeByMarks   

  }

}

object StartProcessLayer {

  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir", "C:\\Users\\1559464\\hadoopLearning\\hadoop");  
    //println("sdfsdfsdf")
    val StartProcessLayer = new StartProcessLayer();
    StartProcessLayer.executeBasicSelect(args)
  }
}