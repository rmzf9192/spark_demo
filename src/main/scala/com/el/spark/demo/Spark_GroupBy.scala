package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/6 17:50
 * @Version V1.0
 */
object Spark_GroupBy {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("FlatMap").set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(config)

    val listRdd:RDD[Int] = sc.parallelize(1 to 9)

    val rddGroup:RDD[(Int,Iterable[Int])] = listRdd.groupBy(_ % 2)

    rddGroup.collect().foreach(println)
  }
}
