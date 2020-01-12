package com.el.spark.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author roman zhangfei
 * @Date 2020/1/12 14:15
 * @Version V1.0
 */
object TakeOrdered {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val makeRDD: RDD[Int] = sc.makeRDD(Array(2,5,4,6,3,8))
    val ordered: Array[Int] = makeRDD.takeOrdered(3)
    ordered.foreach(println)
  }
}
