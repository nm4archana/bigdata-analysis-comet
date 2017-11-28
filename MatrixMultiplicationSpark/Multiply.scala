package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
@SerialVersionUID(123L)
case class Elem ( tag: Short, index: Int, value: Double )
      extends Serializable {}

@SerialVersionUID(123L)
case class Pair ( i: Int, j: Int )
      extends Serializable {}

object Multiply 
{
  def main(args: Array[ String ])
  {
    
     val conf = new SparkConf().setAppName("Multiply")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val m = sc.textFile("/Users/archana/Documents/Projects/CloudComputing/project3/M-matrix-large.txt")
     val m = sc.textFile(args(0))
    .map( 
        line => 
      { 
        val a = line.split(",")
        (a(1).toInt,new Elem(0,a(0).toInt,a(2).toFloat))
       }
      )
      
   // val n = sc.textFile("/Users/archana/Documents/Projects/CloudComputing/project3/N-matrix-large.txt")
       val n = sc.textFile(args(1))
      
    .map( 
        line => 
      {
       val a = line.split(",")
       (a(0).toInt,new Elem(1,a(1).toInt,a(2).toFloat))
      }
      )
      val intr  = m.cogroup(n)
      val res  = m.cogroup(n).flatMap {
       case (key: Int, (l1: Iterable[Elem], l2: Iterable[Elem])) =>
           for {
             e1 <- l1.toSeq
              e2 <- l2.toSeq             
               } 
                 yield  ((e1.index, e2.index) , e1.value*e2.value) 
    }.sortByKey().reduceByKey(_+_).sortByKey()
                                                   
    
     val printRes = res.map { case ((key1 : Int ,key2 : Int) ,value : Double) => (key1 + "," + key2 + "," + value )}
    //printRes.saveAsTextFile("/Users/archana/Documents/Projects/CloudComputing/project3/output")
    printRes.collect().foreach(println)
    printRes.saveAsTextFile(args(2));
    sc.stop()

  }
}
