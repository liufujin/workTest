package com.work.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


// 将rdd转化成dataFrame(利用了反射机制)   DataFrame常用操作
object Demo01 {

  def main(args: Array[String]): Unit = {
    //1.创建sparkSession对象

    val ss: SparkSession = SparkSession.builder().appName("worktest")
      .master("local[1]")
      .getOrCreate()

    val text: DataFrame = ss.read.text("d://person.txt")
    text.show()


    //2.创建sc对象
    val sc: SparkContext = ss.sparkContext
    sc.setLogLevel("warn")
    //3.读取文件
    val file: RDD[String] = sc.textFile("d://hadooptest/person.txt")

    //切分

    val rdd1: RDD[Array[String]] = file.map(_.split(" "))
    //4.将rdd与样例类进行关联

    val personRdd: RDD[Person] = rdd1.map(x=>Person(x(0).toInt,x(1),x(2).toInt))


    //5.创建dataFrame
    //需要隐式转换          !!!

    import ss.implicits._
    val personDF: DataFrame = personRdd.toDF


    personDF.printSchema()
    personDF.show()

    //---------------------DSL风格语法-------------start

/*	val m=2
	val n=3
	println(m+n)
  //  personRdd.printSchema()

    val a=1
    val b=10
    println(a+b)*/

    //println(" ---------------------SQL风格语法-------------start ")



    //关闭

    sc.stop()
    ss.stop()

  }

  case class Person(id:Int,name:String,age:Int)

}
