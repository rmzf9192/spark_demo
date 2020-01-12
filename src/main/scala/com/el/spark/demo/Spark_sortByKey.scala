package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/12 12:41
 * @Version V1.0
 */
object Spark_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Distinct").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    //根据key正序排
    rdd.sortByKey(true).collect().foreach(println)
    //倒序排序
    rdd.sortByKey(false).collect().foreach(println)
  }
}
