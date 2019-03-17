package com.work.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Demo3 {

 //   case class Row(id:Int,name:String,age:Int)    自己指定Schema的方式 不需要自己指定样例类 直接可以导包！
  def main(args: Array[String]): Unit = {
    //1,创建Sparksession对象
    val spark: SparkSession = SparkSession.builder().appName("hello2").master("local[2]").getOrCreate()
    //2,创建sparkcontext对象读取文件 创建rdd
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    val rdd1: RDD[Array[String]] = sc.textFile("d://person.txt").map(_.split(" "))
// 1),rdd1与row进行关联   类似 用样例类 只不过不用自己写   要导包。 Row
    val rowRDD: RDD[Row] = rdd1.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
// 2),创建schema
    val schema=StructType(
      StructField("id",IntegerType,false) ::
      StructField("name",StringType,false) ::
      StructField("age",IntegerType,false) :: Nil
    )
    //3,创建df
    val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema)
    //4   方式 1） DSL风格API语法
    dataFrame.printSchema()
    dataFrame.show()
    //5    方式 2）SQL风格语法
    dataFrame.createTempView("person")  //1）先注册成一张表
    spark.sql("select * from person where age < 30").show() //2）再写sql查询


    sc.stop()
    spark.stop()

  }
}
