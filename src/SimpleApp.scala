import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex


import scala.util.parsing.json.JSON



import java.util

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.parsing.json.JSON


/*
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "E:\\README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
*/

object SimpleApp{

  //case class语句不能放到main函数内部，会报错
  case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    val df= spark.read.json("E:\\software\\spark\\spark-2.1.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    /*
    df.show()
    import spark.implicits._
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" >= 20).show()
    df.groupBy("age").count().show()


    //people1是自定义的临时表的表名，把df转换为数据库表people1
    df.createOrReplaceTempView("people1")
    val sqlDF = spark.sql("select * from people1")
    sqlDF.show()

    df.createGlobalTempView("people1")
    spark.sql("select * from global_temp.people1").show()
    spark.newSession().sql("select * from global_temp.people1").show()
    */

    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect()

    val path = "E:\\software\\spark\\spark-2.1.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()


    val log_str = """112.98.75.255 - - [30/Mar/2018:17:14:07 +0800] \\\"GET /live.aishang.ctlcdn.com/00000110240126_1/encoder/0/playlist.m3u8?CONTENTID=00000110240126_1\\u0026AUTHINFO=X2h6YXZoTcED7UUpMrNw6G24AJV86JNBtFZ1A4gTelpHLxBz0K7JxUpFhMqOlGRX35hOAhZdTXOrK0W8QvBRom+XaXZYzB/QfYjeYzGgKhOoR077xvQnxpugZQRJ9vkoU1XwJFRgrbX+nTs60JauQXHHAWe/ZCBS/indRELWmSo0WWQl5nGJQm2aT33g00ZcuoIyGzQncnyl87mrrEOuKg==\\u0026USERTOKEN=e8uvlRjeDuSGZOsj29hk3bvq5QCGSnvh\\u0026areano=230100\\u0026AUTHINFO=TmsZBlrZlxbARsp9EQQhu%2B4rx5nxiQzOGtH%2BN%2BwdlpJHLxBz0K7JxUpFhMqOlGRX35hOAhZdTXOrK0W8QvBRom%2BXaXZYzB%2FQfYjeYzGgKhOoR077xvQnxpugZQRJ9vkoU1XwJFRgrbX%2BnTs60JauQXHHAWe%2FZCBS%2FindRELWmSpFehhQP3FpAo3AsMaxmdLX85vJS%2BOrbwJ29DbrwOCRsQ%3D%3D\\u0026USERTOKEN=J5%2BXQ%2FNMAuPeqUOARnKvs2jwKHJ61M9Y HTTP/1.1\\\" 200 680 \\\"-\\\" \\\"AppleCoreMedia/1.0.0.7B367 (iPad; U; CPU OS 4_3_3 like Mac OS X)\\\"  request_time 0.004 upstream: 127.0.0.1:8000  200 0.004 - -  -\"""

    val log_string = "112.98.94.136 - - [02/Apr/2018:07:59:59 +0800] \"GET /ts.aishang.ctlcdn.com/00000110240130_1/encoder/0/playlist.m3u8?areano=230800&position=1522624908&CONTENTID=00000110240130_1&timeshift=125&AUTHINFO=ZD%2BmezoReXlZBNlj1bsEqEr7jGyp8Ob%2Fs%2BdlBCeouIDGoF%2BEPMZXNF%2F%2B%2Fa9cuc3D9o%2FDevAQUUJEdhvMRAq0ZJUEROcjM0eASgokpMwdXqmAT23sx8wtKETcsm%2FzwGGKMUf%2ByW83f%2BctPIUYmIlwjbmwKq%2F82sfa7Os7%2B6gTckpMmoSzdbssVPdo8WCdyUCrNwT6k1R8RI6RiTROJ7lC5Q%3D%3D&USERTOKEN=wlS%2FoZQPD68F1%2BdN0yeMEEwjKb3MOSXe HTTP/1.1\" 200 682 \"-\" \"AppleCoreMedia/1.0.0.7B367 (iPad; U; CPU OS 4_3_3 like Mac OS X)\"  request_time 0.092 upstream: 127.0.0.1:8000  200 0.092 - -  -"
    val pattern = new Regex("(S|s)cala")  // 首字母可以是大写 S 或小写 s
    val str = "Scala is scalable and cool"
    val json_str = "{\"A\":\"nihao\",\"B\":\"nibuhao\",\"C\":\"zenmeban\"}"

    println((pattern findAllIn str).mkString(","))
    println((pattern findAllIn(str)).mkString(","))



    val colors = Map("red" -> "hello",
      "azure" -> "world",
      "peru" -> "shijie")
    println(colors.keys)


    val str2 = "{\"name\":\"jeemy\",\"age\":25,\"phone\":\"18810919225\"}"
    val jsonParser = new JSONParser()

    val jsonObj: JSONObject = jsonParser.parse(json_str).asInstanceOf[JSONObject]
    val name = jsonObj.get("A").toString
    println(name)



    val str111 = """112.98.94.136 - - [02/Apr/2018:07:59:59 +0800] "GET /ts.aishang.ctlcdn.com/00000110240130_1/encoder/0/playlist.m3u8?areano=230800&position=1522624908&CONTENTID=00000110240130_1&timeshift=125&AUTHINFO=ZD%2BmezoReXlZBNlj1bsEqEr7jGyp8Ob%2Fs%2BdlBCeouIDGoF%2BEPMZXNF%2F%2B%2Fa9cuc3D9o%2FDevAQUUJEdhvMRAq0ZJUEROcjM0eASgokpMwdXqmAT23sx8wtKETcsm%2FzwGGKMUf%2ByW83f%2BctPIUYmIlwjbmwKq%2F82sfa7Os7%2B6gTckpMmoSzdbssVPdo8WCdyUCrNwT6k1R8RI6RiTROJ7lC5Q%3D%3D&USERTOKEN=wlS%2FoZQPD68F1%2BdN0yeMEEwjKb3MOSXe HTTP/1.1" 200 682 "-" "AppleCoreMedia/1.0.0.7B367 (iPad; U; CPU OS 4_3_3 like Mac OS X)"  request_time 0.092 upstream: 127.0.0.1:8000  200 0.092 - -  -"""
    val pattern1 = """^(?<remoteaddr>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - (?<localtime>\[.*\]) "(?<request>[^"]*)" (?<status>\d+) (?<bodybytessent>\d+) "(?<httpreferer>[^"]*)" "(?<httpuseragent>[^"]*)" (.*)""".r


    //val pattern3 = """^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(.*)\] "GET /([^("|/)]*)" (\d+) (\d+) "([^"]*)" "([^"]*)" (.*)""".r

    //有转义字符\的形式
    //val pattern3 = """^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(.*)\] "GET /([a-z\.]+)(.*) HTTP/1.1" (\d+) (\d+) "([^"]*)" "([^"]*)"  request_time ([0-9.]+) upstream: ([0-9.:]+)  ([0-9]+) ([0-9\.]+) (.*)""".r    //println((pattern3 findAllIn str111))

    //无转移字符\的形式
    val pattern3 = """^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(.*)\] "GET /([a-z.]+)(.*) HTTP/1.1" (\d+) (\d+) "([^"]*)" "([^"]*)"  request_time ([0-9.]+) upstream: ([0-9.:]+)  ([0-9]+) ([0-9.]+) (.*)""".r
    //val pattern3(a1, a2, a3, a4, a5, a6, a7, a8) = str111
    val pattern3(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) = str111
    println(a1)
    println(a2)
    println(a3)
    println(a4)
    println(a5)
    println(a6)
    println(a7)
    println(a8)
    println(a9)
    println(a10)
    println(a11)
    println(a12)
    println(a13)

/*

    val numitemPattern = "([0-9]+) ([a-z]+) ([a-z]+)".r  //通过()区分开，多个子表达式
    val numitemPattern(num, item, tim) = "99 apples dsa"   //单个匹配
    println(num,item, tim)

    val aa = "Runoob"
    val bb = "Baidu"
    val cc = "Google"
    var z = Array(aa, bb, cc)
    println(z)


    val pattern3(a1, b1, c1, d1, e1, f1, g1, h1) = str111
    println(a1)

*/



  }
}