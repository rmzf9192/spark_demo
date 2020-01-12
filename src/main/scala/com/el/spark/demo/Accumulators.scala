package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author roman zhangfei
 * @Date 2020/1/12 13:25
 * @Version V1.0
 */
object Accumulators {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulators").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val textFile: RDD[String] = sc.textFile("in/word.txt")
    val blanklines = sc.accumulator(0)
    val mapRDD: RDD[String] = textFile.flatMap(line => {
      if (line == "") {
        blanklines += 1
      }
      line.split(" ")
    })
    mapRDD.saveAsTextFile("out/output.txt")
    println("Blank lines: " + blanklines.value)
  }

}
