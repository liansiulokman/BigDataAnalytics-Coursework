package polyu.bigdata
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SQLquery {

  def getResults(sc:SparkContext, lower_bound:Int, upper_bound:Int, data_path:String):RDD[(Int, Int, Int)]=
 {     
    //insert your code here
    val textFile = sc.textFile(data_path)
    val lines = textFile.filter(line => line.contains("#"))
    val resultArray = lines.filter(line => ((line.split("#")(3)).toInt>=lower_bound) && ((line.split("#")(3)).toInt<=upper_bound))
    val temp = resultArray.sortBy(line => line.split("#")(1).toInt)
    //val temp = resultArray
    //val temp1 = temp.groupBy{row=>row.split("#")(1)}
    val resultArray2 = temp.map(key => (key.split("#")(1).toInt,key.split("#")(2).toInt,key.split("#")(3).toInt)).sortBy(_._1)
    val temp1 = resultArray2.groupBy{row=>(row._2)}
    //val result1 = resultArray2.reduceByKey((x,y)=> (x._1,y._2,x._3))
    //val result = result1.values
    //val result1 = temp1.reduce((a,b)=>b)
    val result = temp1.mapValues(a=>a.reduce((x,y)=> if(x._3>=y._3 && x._1<y._1) x else y)).values.sortBy(_._2)
    //val result = temp1.mapValues(a=>a.reduce((x,y)=> if(x._3>=y._3 && x._1<y._1)x else y)).values.sortBy(_._2)
    result.sortBy(_._1)
 }
  
  
  def main(args: Array[String]) {     
        
  // We need to use spark-submit command to run this program 
  val conf = new SparkConf().setAppName("Assignment 2")//.setMaster("local")  // uncomment 'setMaster' for local check
  val sc = new SparkContext(conf)  
  
  val Results = getResults(sc, args(1).toInt, args(2).toInt, args(0))
  //val Results = getResults(sc, 80, 120, "/home/bigdata/guestshare/Question/Data/OrderDetails")
  Results.collect().foreach(println)
  println("Number of results: %s".format(Results.count()))
  }

}