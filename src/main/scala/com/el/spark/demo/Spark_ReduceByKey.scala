package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/11 18:11
 * @Version V1.0
 */
object Spark_ReduceByKey {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("FlatMap").set("spark.testing.memory", "2147480000")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))

    //对相同的key值进行相加运算
    val reduceRdd:RDD[(String,Int)] = rdd.reduceByKey((x, y) => x + y)
    reduceRdd.collect().foreach(println)
    println("================================")
    val reduceRdd2:RDD[(String,Int)] = rdd.reduceByKey(_ + _)
    reduceRdd2.collect().foreach(println)

  }
}
