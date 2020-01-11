package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/5 19:45
 * @Version V1.0
 */
object Spark_Map {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Spark Map").set("spark.testing.memory", "2147480000")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val mapRdd:RDD[Int] = sc.makeRDD(1 to 10)

    mapRdd.collect()

    val listRdd:RDD[Int] = mapRdd.map(x => x * 2)
    listRdd.collect().foreach(println)

    val listRdd2:RDD[Int] = mapRdd.map(_ * 2)

    println(listRdd2.collect())

    val arryList = Array(1, 2, 3, 4)

    sc.makeRDD(arryList).map(_ + "test").collect().foreach(println)

    listRdd2.collect().foreach(println)

  }
}
