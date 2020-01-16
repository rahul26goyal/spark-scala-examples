package com.rahulg.bootcamp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReplaceSecretsWords {

  def main(array: Array[String]): Unit = {
    println("This program will demonstrate how we can replace some secrets present in data using broadcast variables...")

    val master = "local"
    val conf = new SparkConf().setAppName("WordCountExample").setMaster(master)
    val sc  = new SparkContext(conf)
    val fileName = "file:/Users/rahulg/learning/bigdata/spark/scala/spark-scala-examples/data/InputDataWithSecret"

    val fileRdd = sc.textFile(fileName)

    replaceSecretInData(fileRdd, sc)

    sc.stop()
  }

  def replaceSecretInData(fileRdd: RDD[String], sc: SparkContext): Unit = {
    val secret = "secret-token"
    val broadcastVariable = sc.broadcast(secret)

    fileRdd.foreach { line =>
      if (line.contains(broadcastVariable.value)) {
        println(line)
      }
    }

    print("Starting map...")
    //val maskedRdd = fileRdd.map { line => line.replaceAll(secret, "XXXXX")}
    val maskedRdd = fileRdd.map { line => line.replaceAll(broadcastVariable.value, "XXXXX")}
    maskedRdd.foreach {l => println(l)}
  }


}
