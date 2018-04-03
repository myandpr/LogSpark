import org.apache.spark._

/**
  * Created by Joe on 2017/1/16.
  */
object WordCount {
  def main(args : Array[String]): Unit = {
    val inputPath = "E:\\hello.txt"
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rowRdd = sc.textFile(inputPath)
    println(rowRdd.take(1).toString)
    val resultRdd = rowRdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1)).reduceByKey(_ + _)
    println(resultRdd.take(2).toString)
    for (data <- resultRdd) {
      println(data)
    }
    sc.stop()
  }
}