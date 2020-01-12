package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

/**
 * @author roman zhangfei
 * @Date 2020/1/12 13:28
 * @Version V1.0
 */
object ReadJson {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val file: RDD[String] = sc.textFile("in/user.json")

    val result  = file.map(JSON.parseFull)

    result.foreach(println)
    val rdd = sc.parallelize(Array((1,2),(3,4),(5,6)))
    rdd.saveAsSequenceFile("out/seqFile")
  }
}
