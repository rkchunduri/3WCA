package com.fca.conceptgeneration

import org.apache.spark.SparkContext

object SampleDemoClass {
   def main(args: Array[String]): Unit = {

  val no = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
  val sc = new SparkContext("local", "sample-demo")
  val data = sc.parallelize(no);
  
    data.map{f=>f*2}.count()
}
   
}