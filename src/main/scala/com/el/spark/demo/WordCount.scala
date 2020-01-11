package com.el.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author roman zhangfei
 * @Date 2020/1/7 20:58
 * @Version V1.0
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("wordCount").set("spark.testing.memory", "2147480000")

    val sc = new SparkContext(config)

    //将文件内容读取
    val lines: RDD[String] = sc.textFile("in/word.txt")
    //将一行一行的数据转换为单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词结构进行转换
    val wordToOne: RDD[(String, Int)] = words.map((_,1))

    //对转换后的结果进行分组聚合
    val reduceByKey: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    //将统计结果打印到控制台
    val result: Array[(String, Int)] = reduceByKey.collect()

    result.foreach(println)
  }
}
