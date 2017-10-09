package org.learning

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class AccumulatorTest(sc: SparkContext) {

  def TestOneWithNormalVariable(args: Array[String]) {

    //val File = sc.textFile("C:\\Users\\1559464\\hadoopLearning\\spark\\input\\accumulatorTest2.txt");
    val File = sc.textFile(args(0));
    val splitFile = File.map(line => line.trim.split("\u0001")(8));
    //splitFile.foreach(println)
    val gr = sc.accumulator(0, "GudRecord")
    val br = sc.accumulator(0, "BadReord")
    splitFile.foreach(word => {
      if (word.equalsIgnoreCase("")) {
        br += 1;
      } else {
        gr += 1;
      }

    })
    val gudRec = splitFile.filter(ln => !ln.equalsIgnoreCase("")).count();
    val badRec = splitFile.filter(ln => ln.equalsIgnoreCase("")).count();

    print("Gudrec: " + gudRec + " , BadRec: " + badRec);
    println()
    print("Gudrec: " + gr.value + " , BadRec: " + br.value);
  }

}

object AccumulatorTest {

  private val conf = new SparkConf().setAppName("My App");
  private val sc = new SparkContext(conf);
  
  def main(args: Array[String]) {

    val accTst = new AccumulatorTest(sc);
    accTst.TestOneWithNormalVariable(args);
  }
}