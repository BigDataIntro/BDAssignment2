package ru.innopolis.model

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, ProbabilisticClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer}
import org.apache.spark.sql.{DataFrame}

import scala.io.Source

object Model {
  def getIndexer(): StringIndexer = {
    val indexer = new StringIndexer()
      .setInputCol("sentiment")
      .setOutputCol("label")
    indexer
  }
  def getTokenizer():RegexTokenizer={
    val tokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")
      .setPattern("\\W")
    tokenizer
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

  def getHashingTF():HashingTF={
    val hashingTF = new HashingTF()
      .setInputCol("tokens").setOutputCol("features")
    hashingTF
  }
  def getClassifier()={
    val clf = new LogisticRegression().setMaxIter(100).setRegParam(0.05)
    clf
  }
  def getPipeline()={
    val indexer = getIndexer()
    val tokenizer = getTokenizer()
    val stopWordsRemover = getStopwordRemover()
    val hashingTF = getHashingTF()
    val classifier = getClassifier()
    val pipeline = new Pipeline().setStages(Array(indexer,tokenizer,stopWordsRemover, hashingTF, classifier))
    pipeline
  }
  def train(pipeline:Pipeline, trainingData:DataFrame)={
    val model = pipeline.fit(trainingData)
    model
  }
  def predict(model:PipelineModel, testDF:DataFrame)={
    // Create the classification pipeline and train the model
    val prediction = model.transform(testDF)
    //      .select("id","label","prediction")

    prediction
  }
  def evaluate(testDF:DataFrame) = {
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
    evaluator.evaluate(testDF)
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
  //  def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))

