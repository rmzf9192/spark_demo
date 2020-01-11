package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/5 19:45
 * @Version V1.0
 */
object Spark_Sample {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Spark Map").set("spark.testing.memory", "2147480000")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[String] = sc.makeRDD(Array("hello1","hello1","hello2","hello3","hello4","hello5","hello6","hello1","hello1","hello2","hello3"))

    val sampleRdd:RDD[String] = rdd.sample(true, 0.7)

    println("放回抽样")
    sampleRdd.foreach(println)

    val sampleRdd1:RDD[String] = rdd.sample(false, 0.7)
    println("不放回抽样")
    sampleRdd1.foreach(println)
  }
}
