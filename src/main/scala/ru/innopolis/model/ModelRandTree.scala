package com.bdtwitter.model

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.DataFrame

import scala.io.Source

object ModelRandTree {
  def getIndexerRandDesT(trainingData:DataFrame): StringIndexer = {
    val labelIndexer  = new StringIndexer()
      .setInputCol("sentiment")
      .setOutputCol("label")
    labelIndexer
  }


  def getTokenizerRandDesT(): Tokenizer = {
    val dtTokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")
    dtTokenizer
  }


  def getStopwordRemover()={
    val src = Source.fromFile("data/stopwords.csv")
    val stopwords = src.getLines().toList
    src.close()
    val stopWordsRemover = new StopWordsRemover()
      .setStopWords(stopwords.toArray)
      .setInputCol("tokens")
      .setOutputCol("clean_tokens")
    stopWordsRemover
  }

  def getHashingTFRandDesT():HashingTF = {
    val dtHashingTF = new HashingTF()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setNumFeatures(100)
    dtHashingTF
  }


  def getClassifierRandDesT() = {
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
    dt
  }

  def getPipelineRandDesT(trainingData:DataFrame)= {
    val pipeline = new Pipeline()
      .setStages(Array(getIndexerRandDesT(trainingData),
        getTokenizerRandDesT(), getHashingTFRandDesT(), getClassifierRandDesT()))
    pipeline
  }


  def trainRandDesT(pipeline:Pipeline, testDF:DataFrame) = {
    val dtModel = pipeline.fit(testDF:DataFrame)
    dtModel
  }

  def predictRandDesT(model:PipelineModel, testDF:DataFrame)= {
    val predictions = model.transform(testDF)
    predictions
  }

  def evaluateRandDesT(model:PipelineModel,testDF:DataFrame) = {
    val dtEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = dtEvaluator.evaluate(predictRandDesT(model, testDF))
    println(s"DT with TFIDF train accuracy = ${accuracy}")
  }


  def eval_metrics(model:PipelineModel, withPredictions:DataFrame) = {
    val predLabels = withPredictions.select("prediction", "label")

    val predLabelsRDD = predLabels.rdd.map(row => (row(0).toString.toDouble, row(1).toString.toDouble))
    predLabelsRDD.collect().foreach(println)

    val metrics = new BinaryClassificationMetrics(predLabelsRDD)

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }


    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val f05Score = metrics.fMeasureByThreshold(beta)
    f05Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }


  }
  def save_model(model:PipelineModel, path:String)={
    model.save(path)
  }
  def save_eval(eval:Double, path:String)={
    import java.io._
    val pw = new PrintWriter(new File(path))
    pw.write(eval.toString)
    pw.close()
  }


  def load_model(path:String):PipelineModel= {
    val model = PipelineModel.load(path)
    model
  }

}
