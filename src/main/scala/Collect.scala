import java.io.File
import java.util.Properties

import com.google.gson.Gson
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import ru.innopolis.model.Model

import scala.io.Source

/** Collect at least the specified number of tweets into json text files. */
object Collect extends App {


  val spark = SparkSession
    .builder
    .appName(getClass.getSimpleName.replace("$", ""))
    .master("spark://10.90.138.32:8088")
    .getOrCreate()

  // val sqlContext = spark.sqlContext

  val sc: SparkContext = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))

  val tweetStream = ssc.socketTextStream("10.91.66.168", 8989).map(new Gson().toJson(_))
  val model = Model.load_model("~/DeploySM/data/models/logreg.model")
  import spark.implicits._
  val predicted = tweetStream.foreachRDD(
    rdd => {Model.predict(model, rdd.toDF())
    rdd.saveAsTextFile("~/DeploySM")}
  )

  ssc.start()
  ssc.awaitTermination()

}