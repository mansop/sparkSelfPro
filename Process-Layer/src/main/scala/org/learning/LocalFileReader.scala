package org.learning

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class LocalFileReader(sc: SparkContext) {
  def readFile(fileLoc: String, delimiter: String): RDD[Array[String]] = {
    val File = sc.textFile(fileLoc)
    val splitFile = File.map(line => line.trim.split(delimiter))
    splitFile
  }

  def readCityFile(): RDD[Array[String]] = {
    readFile("C:\\Users\\1559464\\hadoopLearning\\spark\\input\\city.txt", "\\|")    
  }

  def readStationFile(): RDD[Array[String]] = {
    readFile("C:\\Users\\1559464\\hadoopLearning\\spark\\input\\station.txt", "\\|")    
  }

  def readCountryFile(): RDD[Array[String]] = {
    readFile("C:\\Users\\1559464\\hadoopLearning\\spark\\input\\country.txt", "\\|")    
  }

  def readGradeFile(): RDD[Array[String]] = {
     readFile("C:\\Users\\1559464\\hadoopLearning\\spark\\input\\grade.txt", "\\|")    
  }

  def readStudentFile(): RDD[Array[String]] = {
    readFile("C:\\Users\\1559464\\hadoopLearning\\spark\\input\\student.txt", "\\|")
  }
}