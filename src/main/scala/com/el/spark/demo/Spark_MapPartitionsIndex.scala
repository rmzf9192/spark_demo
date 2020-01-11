package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/6 17:11
 * @Version V1.0
 */
object Spark_MapPartitionsIndex {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("PartitionsIndex").set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(config)

    val listRdd = sc.makeRDD(1 to 10)

    //创建一个MapPartitionsIndex，是每个元素跟所在的分区，形成一个元组，组成一个新的RDD

    val listRdd2 = listRdd.mapPartitionsWithIndex((index, items) => (items.map((index, _))))

    listRdd2.collect().foreach(println)

    println("+=======================+")

    val listRdd3:RDD[(Int,String)] = listRdd.mapPartitionsWithIndex({
      case (index, items) => {
        items.map((_, "分区号：" + index))
      }
    })

    listRdd3.collect().foreach(println)
  }
}
