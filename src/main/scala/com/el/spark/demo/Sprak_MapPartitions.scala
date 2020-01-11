package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/6 17:02
 * @Version V1.0
 */
object Sprak_MapPartitions {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("MapPatitions").set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(config)
    //创建一个数组的RDD
    val mapPartitions:RDD[Int] = sc.makeRDD(1 to 10)

    //mapPartitions 对一个rdd中的所有分区进行遍历
    //优于map,减少发到执行器的交互次数。
    //可能内存溢出
    val listRdd = mapPartitions.mapPartitions(x => {
      x.map(_ * 2)
    })

    listRdd.collect().foreach(println)
  }
}
