package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/7 20:31
 * @Version V1.0
 */
object Spark_SortBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Distinct").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val sortByRdd:RDD[Int] = sc.makeRDD(List(5,1,2 ,3,4))

    val sortBy = sortByRdd.sortBy(x => x)

    sortBy.collect().foreach(println)


  }
}
