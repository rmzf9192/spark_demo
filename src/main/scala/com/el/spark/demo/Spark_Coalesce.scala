package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/7 20:31
 * @Version V1.0
 */
object Spark_Coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Distinct").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val rddList:RDD[Int] = sc.makeRDD(1 to 16, 4)

    val coalescaRdd:RDD[Int] = rddList.coalesce(3)

    coalescaRdd.saveAsTextFile("output")

    println(coalescaRdd.partitions.size)
  }
}
