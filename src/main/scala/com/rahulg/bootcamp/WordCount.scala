package com.rahulg.bootcamp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.io.Directory
import java.io.File

object WordCount {

  def main(array: Array[String]): Unit = {
    println("This program will calculate the number of words present in a file in different ways.")

    //Running Spark locally
    val master = "local"
    val conf = new SparkConf().setAppName("WordCountExample").setMaster(master)
    val sc  = new SparkContext(conf)

    val fileName = "file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/InputData.txt"

    //Transformation 1: to point to input data
    val fileRdd = sc.textFile(fileName)

    calculateWordCountInline(fileRdd)

    //finally stop the SparkDriver.
    sc.stop()
  }

  def calculateWordCountInline(fileRdd : RDD[String]): Unit = {
    //clear tmp directory
    var directoryName = "/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp"
    val directory = new Directory(new File(directoryName))
    directory.deleteRecursively()

    //create RDD Transformation 1: Create a RDD with split words.
    val wordRdd = fileRdd.flatMap(line => line.split(" "))
    //to view the result of this transformation, save it to a file.
    wordRdd.saveAsTextFile("file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp/InputDataFlatMappedResult.txt")

    //create RDD Transformation 2: create a map of word, frequency.
    val wordsRdd  = wordRdd.map(word => (word, 1))
    wordsRdd.saveAsTextFile("file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp/InputDataMappedResult.txt")

    //start computation action..grouping count of same words together.
    val result = wordsRdd.reduceByKey((a, b) => a + b)
    result.saveAsTextFile("file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp/InputDataResult.txt")

    //print results:
    System.out.println("Result  of calculateWordCountInline :")
    result.foreach(line => {
      println(line)
    })

    //get Result count
    val wordCount = result.count
    //val wordCount = result.collect()
    println("Result  of calculateWordCountInline unique words: " + wordCount)
  }
}
