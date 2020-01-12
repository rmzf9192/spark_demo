package com.el.spark.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author roman zhangfei
 * @Date 2020/1/12 19:14
 * @Version V1.0
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    //sparkSession,不能直接new，私有的对象,具体看源码
    val spark = SparkSession.builder.config(conf).getOrCreate()
  /*  //创建SparkConf()并设置App名称
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()*/

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("in/user.json")

    // Displays the content of the DataFrame to stdout
    df.show()

    df.filter($"age" > 21).show()

    df.createOrReplaceTempView("persons")

    spark.sql("SELECT * FROM persons where age > 21").show()

    spark.stop()

  }
}
