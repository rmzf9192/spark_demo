package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/11 17:41
 * @Version V1.0
 */
object Spark_Union {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Distinct").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    //union 并集
    val rdd1:RDD[Int] = sc.parallelize(1 to 5)
    val rdd2:RDD[Int] = sc.parallelize(5 to 10)

    val listRdd:RDD[Int] = rdd1.union(rdd2)
    println("并集")
    listRdd.collect().foreach(print)
    //求差集
    val rdd4 = sc.parallelize(3 to 8)
    val rdd5 = sc.parallelize(1 to 5)
    var subrdd = rdd4.subtract(rdd5)
    println("差集")
    subrdd.collect().foreach(print)
    //求交集
    val rdd6 = sc.parallelize(1 to 7)
    val rdd7 = sc.parallelize(5 to 10)
    val rdd8 = rdd6.intersection(rdd7)
    println("交集")
    rdd8.collect().foreach(print)
    //笛卡尔积
    val rdd9 = sc.parallelize(1 to 3)
    val rdd10 = sc.parallelize(2 to 5)
    val rdd11 = rdd9.cartesian(rdd10)
    println("笛卡尔积")
    rdd11.collect().foreach(print)

    //将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
    val rdd12 = sc.parallelize(Array(1,2,3),3)
    val rdd13 = sc.parallelize(Array("a","b","c"),3)

    val rddInt:Array[(Int,String)] = rdd12.zip(rdd13).collect()
    val rddString:Array[(String,Int)] = rdd13.zip(rdd12).collect()
    rddInt.foreach(println)
    println("==================")
    rddString.foreach(println)
  }

}
