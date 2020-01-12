package com.el.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/12 15:24
 * @Version V1.0
 */
object Spark_cache {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array("atguigu"))
    val cache =  rdd.map(_.toString+System.currentTimeMillis).cache

    cache.collect.foreach(println)
    cache.collect.foreach(println)
    cache.collect.foreach(println)
  }
}
