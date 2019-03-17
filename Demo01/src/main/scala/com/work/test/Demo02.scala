package com.work.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


//3,  用样例类    把每行封装成一个对象    要写在类外面！！！
case class Person(id:Int,name:String,age:Int)


object Demo02 {



  def main(args: Array[String]): Unit = {

    //1,用sc读取文件 生成rdd
    val ssc: SparkSession = SparkSession.builder().master("local[2]").appName("hello").getOrCreate()
    val sc: SparkContext = ssc.sparkContext
    val textFile: RDD[String] = sc.textFile("/person.txt")

    //2,对读取的文件切分    返回  RDD[Array[String]] 类型
    val mapRDD: RDD[Array[String]] = textFile.map(_.split(" "))

    //4,关联样例类！！！    遍历切分的数组 转换成封装对象的格式  RDD[Person]，改写每个元素的数据类型
    val mapRDD2: RDD[Person] = mapRDD.map(x=>Person(x(0).toInt,x(1),x(2).toInt))

    //5，转换成df 隐式转换 注意ssc的名字不能写成别的 。

    import ssc.implicits._

    val personDF: DataFrame = mapRDD2.toDF
    personDF.select("name")
//打印
    personDF.foreach(x=>println(x))
    personDF.foreach(x=>println(x.getInt(1)))
    personDF.foreach(x=>println(x.getInt(0)))

    personDF.select($"age"+1).show

    personDF.filter($"age">30).show

    val num: Long = personDF.filter(x=>x.getAs[Int]("age")>30).count

    println(num)

    personDF.groupBy("age").count.show

//第二种语法：sql
    //1,注册成一张表
    personDF.registerTempTable("person")

    //2，写sql处理
    ssc.sql("select * from person").show


//    创建一个 dataset   通过一个rdd创建
    val dataset: Dataset[String] = ssc.createDataset(sc.textFile("/person.txt"))


//通过一个list集合创建dataset
    List(1,2,3,4,5,6).toDS


//关闭
    ssc.stop()
    sc.stop()

  }
}
