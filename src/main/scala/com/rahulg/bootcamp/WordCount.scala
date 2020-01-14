package com.rahulg.bootcamp

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(array: Array[String]): Unit = {
    println("This program will calculate the number of words present in a file.")

    val master = "local"
    val conf = new SparkConf().setAppName("WordCountExample").setMaster(master)
    val sc  = new SparkContext(conf)
    val fileName = "file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/InputData.txt"

    val fileRdd = sc.textFile(fileName)

    val wordsRdd = fileRdd.flatMap(lines => lines.split(" ")).map(words => (words, 1)).reduceByKey((a, b) => a + b)

    val wordCount = wordsRdd.count()

    System.out.println("Result  of word Count: " + wordCount)
    sc.stop()
  }

}
