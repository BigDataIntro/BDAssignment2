import com.google.gson.Gson
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import ru.innopolis.model.Model

object  Main extends App {
  def Run()={
    // Init Spark
    val conf = new SparkConf().setMaster("local[*]").setAppName("DC")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Loading the text file using sc.textFile function and creating an RDD
    // RDD shape: “CleanedText”,Category”
    val input_path = "data/dataset/train.csv"
    val input_DF = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load(input_path)
      .toDF("id","sentiment","text")

    // Slicing the data into 70:30 ratio for training and testing data
    val Array(trainingData, testData) = input_DF.randomSplit(Array(0.7, 0.3))

    // print the training data
    //    trainingData.show()

    val pipeline = Model.getPipeline()
    val model = Model.train(pipeline, trainingData)
    //    println("Loading model")
    //    val model = Model.load_model("data/models/logreg.model")
    val prediction = Model.predict(model,testData)
    val evaluated = Model.evaluate(prediction)
    prediction.show(false)
    printf("Accuracy: %f \n",evaluated)
    println("Hello world!")

    println("Saving model")
    Model.save_model(model,"data/models/logreg_model")
    Model.save_eval(evaluated, "data/models/logreg.csv")
    //
    println("Loading model")
    val loaded_model = Model.load_model("data/models/logreg_model")
    //    print(loaded_model)

    val prediction1 = Model.predict(loaded_model,testData)
    val evaluated1 = Model.evaluate(prediction1)
    prediction1.show(false)
    printf("Accuracy: %f \n",evaluated1)
    println("Hello world!")
  }

  override def main(args: Array[String]): Unit = {
    Run()
    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName.replace("$", ""))
      .master("spark://172.19.0.2:7077")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val tweetStream = ssc.socketTextStream("10.91.66.168", 8989)//.map(new Gson().toJson(_))
    val model = Model.load_model("data/models/logreg_model")
    import spark.implicits._
    import java.io._
    val predicted = tweetStream
      .foreachRDD(foreachFunc = (rdd, time) => {
        if (rdd.collect().length != 0) {
          val fw = new FileWriter("result.csv", true)
          val pw = new PrintWriter(fw)
          val pred = Model.predict(model, rdd.toDF("text"))
          pw.append(time.toString())
          pw.append(" ")
          rdd.collect().foreach(pw.append)
          pw.append(" ")
          pred.select("prediction").rdd.collect().foreach(c=>pw.append(c.toString()))
          pw.append("\n")
          pw.close()
        }
      })

    ssc.start()
    ssc.awaitTermination()

  }

}