package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/12 12:44
 * @Version V1.0
 */
object Spark_mapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapValues").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    rdd3.mapValues(_+"|||").collect().foreach(println)
  }
}
