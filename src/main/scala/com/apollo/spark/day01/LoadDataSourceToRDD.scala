package com.apollo.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

object LoadDataSourceToRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("三国志")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("data/battle/input/")

    

    rdd1.foreach(e=>println(e))
    //如果参数e在函数中只用了一次 则可以省略 变成一个下划线
    rdd1.foreach(println(_))
    //而且println的括号也可以不要
    rdd1.foreach(println _)
    rdd1.foreach(println)



  }
}
