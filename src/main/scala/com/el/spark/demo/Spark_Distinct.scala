package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/6 17:50
 * @Version V1.0
 */
object Spark_Distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Distinct").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    //去重算子
   /* val makeRDD: RDD[Int] = sc.makeRDD(Array(1,3,2,5,6,3,2,1))
    val makedist: RDD[Int] = makeRDD.distinct()
    makedist.collect().foreach(println)*/

    //输出到文件   disinct有shuffle过程
    val makeRDD: RDD[Int] = sc.makeRDD(Array(1,3,2,5,6,3,2,1),2)
    val makedist: RDD[Int] = makeRDD.distinct()
    makedist.saveAsTextFile("output")

  }

}
