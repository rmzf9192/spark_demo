package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/12 12:51
 * @Version V1.0
 */
object Spark_cogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cogroup").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    rdd.cogroup(rdd1).collect().foreach(println)
  }
}
