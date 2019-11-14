import org.apache.spark.sql.SQLContext
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
    Model.save_model(model,"data/models/logreg.model")
    Model.save_eval(evaluated, "data/models/logreg.csv")
    //
    println("Loading model")
    val loaded_model = Model.load_model("data/models/logreg.model")
    //    print(loaded_model)

    val prediction1 = Model.predict(loaded_model,testData)
    val evaluated1 = Model.evaluate(prediction1)
    prediction1.show(false)
    printf("Accuracy: %f \n",evaluated1)
    println("Hello world!")

  }

  override def main(args: Array[String]): Unit = {
    Run()
  }

}