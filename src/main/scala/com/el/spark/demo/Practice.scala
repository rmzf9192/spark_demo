package com.el.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author roman zhangfei
 * @Date 2020/1/12 12:54
 * @Version V1.0
 */
object Practice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val file: RDD[String] = sc.textFile("in/agent.log")

    //统计每个省份广告被点击次数top3
//    val mapValues:RDD[(String,String)] = file.map(line => (line.split(" ")(1), line.split(" ")(4)))
//    mapValues.collect().foreach(println)
    //3.按照最小粒度聚合：((Province,AD),1)
    val splitValue = file.map { x =>
      val feids: Array[String] = x.split(" ")
      ((feids(1), feids(4)), 1)
    }

    splitValue.collect().foreach(println)
    //4.计算每个省中每个广告被点击的总数：((Province,AD),sum)
    val reduceByValues:RDD[((String,String),Int)] = splitValue.reduceByKey(_ + _)
    reduceByValues.collect().foreach(println)
    //5.将省份作为key，广告加点击数为value：(Province,(AD,sum))
    val mapRdd:RDD[(String,(String,Int))] = reduceByValues.map(x => (x._1._1, (x._1._2, x._2)))
    mapRdd.collect().foreach(println)
    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val groupByKeyRdd:RDD[(String,Iterable[(String,Int)])] = mapRdd.groupByKey()
    groupByKeyRdd.collect().foreach(println)
    //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
   val provinceAdTop3 = groupByKeyRdd.mapValues{x =>
      println(x)
      x.toList.sortWith((x,y)=>x._2 > y._2).take(3)
    }
    provinceAdTop3.collect().foreach(println)
    sc.stop()
  }
}
