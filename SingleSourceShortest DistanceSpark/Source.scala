package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.Map

object Source 
{
  def main(args: Array[ String ]) 
  {
     val conf = new SparkConf().setAppName("Join")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    var distanceMap = collection.mutable.Map[Int, Long]()
    var maxVal = Long.MaxValue

    //val dist = sc.textFile("/Users/archana/Documents/Projects/CloudComputing/project_4/small-graph.txt")
    val dist = sc.textFile(args(0))
    .map( 
        line => 
      { 
   
        val a = line.split(",")
        if(a(0).toInt==0)
         (a(0).toInt -> 0)
         else
        (a(0).toInt -> maxVal.toLong)
       }
      )
      val distance = dist.distinct();
      var m = distance.collectAsMap()
      
      //var input = sc.textFile("/Users/archana/Documents/Projects/CloudComputing/project_4/small-graph.txt")
      var input = sc.textFile(args(0))
    .map( 
        line => 
      {                                            
        val a = line.split(",")     
        (a(2).toInt,a(1).toLong,a(0).toInt)
       }
      )
       
     val edgeGroup = input.groupBy(k => k._1) 
      
        val finalEdgeGroup =  edgeGroup.map(
        line => 
          {
            
            if(line._1!=0){
            (line._1,Long.MaxValue,line._2)
            }
            else{
               (line._1,0.longValue(),line._2)
            }  
          }
      )
     
     
     
      var fin = finalEdgeGroup
      for(i<-1 to 4)
      {  
        var t = fin.map(
          line=>  {
                      (line._1,line._2)
            })
         m = t.collectAsMap()  
        
         fin =  fin.map(
       line =>
         {          
            var dist = m.get(line._1.toInt).get.toString().toLong
           
            for(i <- line._3)
            {    
              //3,2,2  -> j,d,i
             dist = m.get(i._1.toInt).get.toString().toLong
    
            //  val value = m.get(i._3.toInt).get.toString().toLong
            
              var value = m.getOrElse(i._3.toInt, Long.MaxValue).toString().toLong
              
              if(value!=Long.MaxValue && dist > value+i._2.toLong)
              {
             
                 dist = value+i._2.toLong
                 m = m.-(i._1.toInt)
                 m =  m.updated(i._1.toInt,dist)   
             
              
              }                   
            }                    
          
            (line._1,dist,line._3)                
         }    
       )
        
      }
            
          fin = fin.sortBy(line =>line._1)
          var printRes = fin.map { case (key : Int ,val1 : Long ,val2 : Iterable[(Int,Long,Int)]) => (key + " " + val1) }
          //printRes =  printRes.sortBy(line =>line(0).toInt)
          //printRes.saveAsTextFile("/Users/archana/Documents/Projects/CloudComputing/project_4/output")
          printRes.collect().foreach(println)
          printRes.saveAsTextFile(args(1))
      
     
  }
}
