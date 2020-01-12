package com.el.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/12 14:04
 * @Version V1.0
 */
object Spark_reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    //reduce
    val makeRDD: RDD[Int] = sc.makeRDD(1 to 20, 3)
    val reduce1: Int = makeRDD.reduce(_ + _)
    println(reduce1)

    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 2), ("c", 3), ("d", 4)))
    val reduce = rdd.reduce((x, y) => {
      println(x,y)
      (x._1+y._1, x._2 + y._2)
    })
    println(reduce)
    //collect算子
    val collect = sc.parallelize(1 to 10)
    println(collect.collect.mkString(","))
    //count算子
    val count = sc.parallelize(1 to 10)
    println(count.count)
    //first算子
    val first = sc.parallelize(1 to 10)
    println(first.first)
    //take算子
     val take = sc.parallelize(Array(2,5,4,6,8,3))
     println(take.take(3).mkString(","))
    // aggregate算子
    val aggregate: RDD[Int] = sc.makeRDD(1 to 10,2)
    println(aggregate.aggregate(0)(_ + _, _ + _))
    //fold算子
    println(aggregate.fold(0)(_ + _))

    //countByKey算子
    val countByKey = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    println(countByKey.countByKey())
  }
}
