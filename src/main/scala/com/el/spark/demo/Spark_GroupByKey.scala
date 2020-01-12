package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/6 17:50
 * @Version V1.0
 */
object Spark_GroupByKey {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("FlatMap").set("spark.testing.memory", "2147480000")

    //创建spark上下文对象
    val sc = new SparkContext(config)
    //创建rdd，使每个元素跟所在的分区形成一个元组
    val words = Array("one","two","three","one","two","three")
    val listRdd: RDD[(String, Int)] = sc.makeRDD(words).map(word => (word,1))
    //groupByKey算子
    val group: RDD[(String, Iterable[Int])] = listRdd.groupByKey()
    group.collect().foreach(println)

    //reduceByKey 算子
    val reduceByKeyRdd:Array[(String,Int)] = listRdd.reduceByKey(_ + _).collect()

    reduceByKeyRdd.foreach(println)
  }
}
