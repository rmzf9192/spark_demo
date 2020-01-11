package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/6 17:29
 * @Version V1.0
 */
object Spark_FlatMap {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("FlatMap").set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(config)

    val listRDD:RDD[Array[Int]] = sc.makeRDD(List(Array(1, 2), Array(3, 4, 5)))
    val listRDD2:RDD[Int] = sc.makeRDD(1 to 10)

    for(i <- listRDD){
      println(i)
    }

    val listRDD1:RDD[Int] = listRDD.flatMap(datas => datas)
    listRDD1.collect().foreach(println)
    val listRdd:RDD[Int] = listRDD2.flatMap(1 to _)
    println("+++++++++++++++++++++++++++++=")
    for(i <- listRdd){
      println(i)
    }

    listRDD1.collect().foreach(println)
  }
}
