package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/6 19:38
 * @Version V1.0
 */
object Spark_Fliter {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Fliter").set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(config)

    val listRdd:RDD[Int] = sc.makeRDD(1 to 10)

    val listRdd1:RDD[Int] = listRdd.filter(_ % 2 == 1)

    listRdd1.collect().foreach(println)
  }
}
