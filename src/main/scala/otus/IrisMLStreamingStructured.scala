package otus

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object IrisMLStreamingStructured {


  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Iris")
      .config("spark.master", "local")
      .getOrCreate()

    val irisDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/IRIS.csv");


    val vectorAssembler = new  VectorAssembler()
      .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width")).setOutputCol("features")

    var vectorModel = vectorAssembler.transform(irisDF)

    vectorModel = vectorModel.drop("sepal_length", "sepal_width", "petal_length", "petal_width")

    val indexer = new StringIndexer()
      .setInputCol("species")
      .setOutputCol("labelIndex")

    val indexFit = indexer.fit(vectorModel)

    val indexModel = indexFit.transform(vectorModel)


    val decisionTreeClassifier = new DecisionTreeClassifier()
        .setLabelCol("labelIndex")
        .setFeaturesCol("features")

    val model = decisionTreeClassifier.fit(indexModel)

    model.write.overwrite().save("./src/main/resources/model")

    // Создаём свойства Producer'а для вывода в выходную тему Kafka (тема с расчётом)
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:19092")

    val inputVectorAssembler = new  VectorAssembler()
      .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width")).setOutputCol("features")


    import spark.implicits._


    // Читаем входной поток
    val readingValues = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:19092")
      .option("subscribe", "input")
      .option("failOnDataLoss", "false")
      .load()

     //выполняется
    val input = readingValues
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.replace("\"", "").split(","))
      .map(Data(_))


    val inputVector = inputVectorAssembler.transform(input)
    val result = model.transform(inputVector)
    val converter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("classification")
      .setLabels(indexFit.labelsArray.flatten)

    val convertedClass = converter.transform(result)


    val query = convertedClass
      .select(concat_ws(",", $"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"classification").as("value"))
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:19092")
      .option("topic", "prediction")
      .option("topic", "prediction")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()


    query.awaitTermination()

  }
}
