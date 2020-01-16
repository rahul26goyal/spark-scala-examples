package com.rahulg.bootcamp

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.io.Directory

object TotalWordCount {

  def main(array: Array[String]): Unit = {
    println("This program will calculate the Total number of words present in a file in different ways.")
    //Running Spark locally
    val master = "local"
    val conf = new SparkConf().setAppName("WordCountExample").setMaster(master)
    val sc  = new SparkContext(conf)

    val fileName = "file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/InputData.txt"

    val fileRdd = sc.textFile(fileName)
    //calculateTotalWordCountInLine(fileRdd)
    //calculateTotalWordCountWithoutAccumulators(fileRdd, sc)
    calculateTotalWordCountWithAccumulators(fileRdd, sc)

    sc.stop()
  }

  def calculateTotalWordCountInLine(fileRdd : RDD[String]): Unit = {
    //clear tmp directory
    var directoryName = "/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp"
    val directory = new Directory(new File(directoryName))
    directory.deleteRecursively()

    //create RDD Transformation 1: Create a RDD with split words.
    val wordRdd = fileRdd.flatMap(line => line.split(" "))
    wordRdd.saveAsTextFile("file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp/InputDataFlatMappedResult.txt")

    //create RDD Transformation 2: create a map of word, frequency.
    val wordsRdd  = wordRdd.map(word => (word, 1))
    wordsRdd.saveAsTextFile("file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp/InputDataMappedResult.txt")

    //start computation action..grouping count of same words together.
    val resultRdd = wordsRdd.reduceByKey((a, b) => a + b)
    resultRdd.saveAsTextFile("file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp/InputDataResult.txt")

    val countRDD = resultRdd.map(a => a._2)
    countRDD.saveAsTextFile("file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/tmp/InputDataFrequencyResult.txt")

    var wordCount = countRDD.sum();

    println("Result  of calculateTotalWordCountInLine unique words: " + wordCount)
  }

  def calculateTotalWordCountWithoutAccumulators(fileRdd : RDD[String], sc: SparkContext): Unit = {
    //initialize a variable
    var wordCountAccum : Int = 0;

    //start computation
    fileRdd.foreach { line =>
      val words  = line.split(" ")
      //update variable
      wordCountAccum += words.length
      println(line + ":" + words.length + ":" + wordCountAccum)
    }

    //drive program: read accumulated value.
    val wordCount = wordCountAccum
    println("Result  of calculateTotalWordCountWithoutAccumulators : " + wordCount)
  }

  def calculateTotalWordCountWithAccumulators(fileRdd : RDD[String], sc: SparkContext): Unit = {
    //initialize a accumulator
    var wordCountAccum = sc.accumulator(0, "Word_Count")

    //start computation
    fileRdd.foreach { line =>
      val words  = line.split(" ")
      //update accumulator
      wordCountAccum += words.length
      println(line + ":" + words.length + ":" + wordCountAccum)
    }

    //drive program: read accumulated value.
    val wordCount = wordCountAccum.value
    println("Result  of calculateTotalWordCountWithAccumulators : " + wordCount)
  }
}
