package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author roman zhangfei
 * @Date 2020/1/12 13:26
 * @Version V1.0
 */
object BasicAvg {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BasicAvg").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val input: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val result: (Int, Int) = computeAvg(input)
    val avg = result._1 / result._2.toFloat
    println(avg)

  }
  def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
  }
}
