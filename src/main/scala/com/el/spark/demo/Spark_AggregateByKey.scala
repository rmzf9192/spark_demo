package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/11 18:20
 * @Version V1.0
 */
object Spark_AggregateByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a",3),("c",4),("b",3),("c",6),("c",8),("a",2)),2)
    val aggregateByKey:RDD[(String,Int)] = rdd.aggregateByKey(0)(Math.max(_, _), _ + _)

    aggregateByKey.collect().foreach(println)
  }
}
