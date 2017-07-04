package assignment3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import scala.io.Source

object Main {

  def main(args: Array[String]): Unit = {
    // We need to use spark-submit command to run this program
    val conf = new SparkConf().setAppName("COMP4434 Assignment 3");
    val sc = new SparkContext(conf);
    //val logFile = "./src/assignment3/Main.scala"
    //val sc = new SparkContext("local", "Assignment 3")
    sc.setLogLevel("WARN")

    var totalPoints = 0
    val dir = "dat/"
    var caseID = 1;
    var correctCaseNum = 0;

    // iterate through all dataset
    val dataIDs = Array(0, 1);
    val numClassList = Array(3, 8);	// the first dataset has 3 classes, the second dataset has 8 classes
    for (dataID <- dataIDs) {

      // Load and parse train data in LIBSVM format.
      val trainingFile = dir + "dataset" + dataID + "_training.txt"
      val trainingData = MLUtils.loadLibSVMFile(sc, trainingFile).cache()

      // Initialization
      val classifier = new OneVsAllLogisticRegression(numClassList(dataID))

      // Train the classifier (which incorporates x logistic regression models)
      train(classifier, trainingData)

      // Prediction for the unseen data
      val testFile = dir + "dataset" + dataID + "_testing.txt"
      val testData = MLUtils.loadLibSVMFile(sc, testFile).cache()

      val predictionAndLabels = testData.map {
        case LabeledPoint(label, features) =>
          val prediction = classifier.predict(features)
          (prediction, label)
      }

      // F1 score
      println("-" * 50)
      println("Result for dataset " + dataID + ":")
      val F1 = classifier.calF1Score(predictionAndLabels)
      println("F1 = " + F1)
      println("-" * 50)
      println("-" * 50)

      
      // prepare information for test cases 
      val gradingFile = dir + "dataset" + dataID + "_expected.txt"
      val lines = Source.fromFile(gradingFile).getLines.toArray
      
      // test cases
      println("Start test case " + caseID);

      val F1_expected = lines(0).toDouble	//the first line of gradingFile is the expected F1 score
      if ((F1 - F1_expected).abs < 0.000001) {
        println("calF1Score() passed")
        totalPoints = totalPoints + 25
        correctCaseNum = correctCaseNum + 1
      } else {
        println("calF1Score() failed")
      }

      println("End test case " + caseID);
      println("-" * 50)

      caseID = caseID + 1
            
      println("Start test case " + caseID);

      var isPredictionExpected = true
      val pairs = predictionAndLabels.collect
      pairs.indices.foreach { index =>
        isPredictionExpected = isPredictionExpected && (pairs(index)._1.toInt == lines(1 + index).toInt)
      }
      
      if (isPredictionExpected) {
        println("transform() and predict() passed")
        totalPoints = totalPoints + 25
        correctCaseNum = correctCaseNum + 1        
      } else {
        println("transform() or predict() failed")
      }
      println("End test case " + caseID);
      println("-" * 50)
      
      caseID = caseID + 1      
    }
    println("You have passed " + correctCaseNum + "/4 test cases")    
	println("You have obtained " + totalPoints + "/100.0 points based on the provided datasets")
  }
  
  /**
   * Initialize *numClass* LR models for binary classification,
   * train these models based on corresponding transformed dataset
   * 
   * Note: DO NOT modify this function
   * 
   * @param trainData the training data
   */
  def train(classifier: OneVsAllLogisticRegression, trainData: RDD[LabeledPoint]) = {
    classifier.models = new Array[LogisticRegressionModel](classifier.numClass)

    classifier.models.indices.foreach { index =>
      val transformedTrainData = classifier.transform(trainData, index)
      classifier.models(index) = new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(transformedTrainData)
    }
  }  

}











