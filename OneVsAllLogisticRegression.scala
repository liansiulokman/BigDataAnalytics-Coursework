package assignment3

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionModel }
import org.apache.spark.rdd.RDD

import scala.collection.Map


/**
 * @brief This class implements logistic regression for multi-class classification problems
 * @param numClass number of classes
 */
class OneVsAllLogisticRegression(_numClass: Int) extends java.io.Serializable {
  val numClass = _numClass
  
  /**
   * Internally, we use an array (i.e., numClass) of LogisticRegressionModel .
   *
   * Note: DO NOT modify this initialization
   */
  var models: Array[LogisticRegressionModel] = null
  
  /**
   * Transform the original dataset by changing the label to either 1 (belong to class *index*) or 0 (not belong to class *index*)
   */
  def transform(trainData: RDD[LabeledPoint], index: Int): RDD[LabeledPoint] = {

    /*
     * This is a FAKE implementation for compilation purpose.
     * Please replace it with your own implementation !!!
     */
   /*trainData.map {
      case LabeledPoint(label, features) =>
        LabeledPoint(0, features)
    }.cache
    * 
    */
    //println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    /*trainData.cache.collect.foreach{row=>
      val arr = row.features.toArray
      for(a<-arr){
        println(a)}
      }
      * 
      */
    /*
      val t = trainData.map{row=>val a = row.features.toArray
      var b = Array(a(3).toDouble)
      for(i<- 4 to a.length-1){
        b = b.union(Array(a(i).toDouble))
      }
      (row.label,b)}
      * 
      */
    //val t = trainData.map{row=> val a = row.features
    //  (row.label,a(2))}
    //trainData.collect.foreach{line=>
    //  println(line.features)
    //}
    val data = trainData.map { 
      case LabeledPoint(label, features) =>
        if(label.toInt==index) LabeledPoint(1, features) else LabeledPoint(0, features)
    }
    data.cache
  }

  /**
   * Return the predicted label given an unseen point
   * Hint: utilize the clearThreshold() function of the LogisticRegressionModel objects 
   * (cf.https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionModel)
   * 
   * @param inFeatures an unseen point
   */
  def predict(inFeatures: Vector): Double = {
    //predict the result from vector of features
    var prediction: LogisticRegressionModel = null
    var max: Double = 0.0
    models.foreach{model=>
      model.clearThreshold
      val predValue = model.predict(inFeatures)
      if(predValue>max){
        max = predValue.toDouble
        prediction = model
      }
    }
    models.indexOf(prediction)
  }

  
  /**
   * Return F1-Score 
   * 
   * @param predictionAndLabels the predicted result and ground truth
   */
  def calF1Score(predictionAndLabels: RDD[(Double, Double)]): Double = {
    //val dataParsed = predictionAndLabels.toArray
    //predictionAndLabels.collect.foreach(println)
    var F1 = 0.0
    val temp = predictionAndLabels.map(line=> if(line._1==line._2) (line,1) else (line,0))
    temp.collect.foreach{i=>
      F1 = F1+(i._2.toDouble)
    }
    F1/temp.count
  }
}