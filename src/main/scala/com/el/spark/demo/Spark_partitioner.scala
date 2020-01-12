package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/12 15:39
 * @Version V1.0
 */
object Spark_partitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapValues").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(Array((1,1),(2,2),(3,3),(4,4),(5,5),(6,6)))
    val par = data.partitionBy(new DomainNamePartitioner(2))
    println(par.mapPartitionsWithIndex((index,items)=>items.map((index,_))).collect)
  }
}
