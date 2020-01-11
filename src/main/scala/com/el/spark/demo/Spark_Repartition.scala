package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/7 20:31
 * @Version V1.0
 */
object Spark_Repartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("repartition").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val rddList:RDD[Int] = sc.makeRDD(1 to 16, 4)
    print(rddList.partitions.size)

    val repartitionRdd:RDD[Int] = rddList.repartition(3)

    repartitionRdd.saveAsTextFile("output")

    println(repartitionRdd.partitions.size)

    val rddGlom:RDD[Array[Int]] = repartitionRdd.glom()


    rddGlom.collect().foreach(datas =>{
       println(datas.mkString+",")
    })
  }
}
