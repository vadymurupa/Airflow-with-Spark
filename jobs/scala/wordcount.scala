import org.apache.spark.sql.SparkSession

object WordCount {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("word count ").master("spark://spark-master:7077").getOrCreate()
        val sc = spark.sparkContext

        val textData = sc.parallelize(List("Hello world", "Hello Scala", "Hello Docker", "Hello Vadym"))

        val counts = textData.flatMap(line => line.split(" "))
                                            .map(word => (word, 1))
                                            .reduceByKey(_ + _)
        counts.collect().foreach(println)

        spark.stop() 
    }
}