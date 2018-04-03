import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

//导入json-smart，https://code.google.com/archive/p/json-smart/downloads
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser


//添加了（1）kafka的jar包和（2）spark_streaming_kafka的jar包
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object StreamingTest {
  def main(args: Array[String]) {

    //获取json形式的String中对应key的value
    def getValueByKey(jsonString: String, targetKey: String):String = {
      val jsonParser = new JSONParser()
      val jsonObj: JSONObject = jsonParser.parse(jsonString).asInstanceOf[JSONObject]
      val targetValue = jsonObj.get(targetKey).toString
      targetValue
    }
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "116.211.156.16:5044,116.211.156.17:5044,116.211.156.18:5044",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("jiamushi003")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    //正则，有名字分组
    //val pattern1 = """^(?<remoteaddr>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - (?<remoteuser>(\s+)|-) (?<localtime>[^"]*) "(?<request>[^"]*)" (?<status>\d+) (?<bodybytessent>\d+) "(?<httpreferer>[^"]*)" "(?<httpuseragent>[^"]*)"""".r

    /*正则表达式字符串，如果里面有特殊字符，可以用""""""三引号引用，这样内部就不需要用转义字符了
      正则匹配remote_addr, remote_user(已去掉), localtime, request, status, body_bytes_send, http_referer, http_user_agent*/
    //val pattern = """^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(.*)\] "([^"]*)" (\d+) (\d+) "([^"]*)" "([^"]*)" (.*)""".r
    val pattern = """^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(.*)\] "GET /([a-z.]+)(.*) HTTP/1.1" (\d+) (\d+) "([^"]*)" "([^"]*)"  request_time ([0-9.]+) upstream: ([0-9.:-]+)  ([0-9-]+) ([0-9.-]+) (.*)""".r

    //有5种字段@timestamp,message,offset,source,source_id,source_ip,type;其中source_ip和record.key是一样的
    //val events = stream.filter(record => record.value.contains("nginx-access")).map(record => (getValueByKey(record.value(), "message"))).print(2)


    val events = stream.filter(record => record.value.contains("nginx-access")).map(record => (getValueByKey(record.value(), "source_ip"), {val logStr = getValueByKey(record.value(), "message")
    val pattern(remote_addr, localtime, channel, url, status, body_bytes, http_referer, http_user_agent, request_time, upstream, upstream_status, upstream_time, last) = logStr
      (remote_addr, localtime, channel, url, status, body_bytes, http_referer)})).count().print()


    ssc.start()
    ssc.awaitTermination()
  }
}

















//string转json
/*
import scala.util.parsing.json._
import scala.reflect.runtime.universe._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.parsing.json.JSON
*/
//正则匹配
/*
import scala.util.matching.Regex
import org.apache.spark.streaming.kafka._
*/

//设置数据流来源
/*
val lines = ssc.socketTextStream("222.85.96.119", 9999, StorageLevel.MEMORY_AND_DISK_SER)
val lines = ssc.socketTextStream("222.85.96.119", 9999)
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
wordCounts.print()
ssc.start()
ssc.awaitTermination()
*/

/*将String转换为json格式
val jsonParser = new JSONParser()
val jsonObj: JSONObject = jsonParser.parse(json_str).asInstanceOf[JSONObject]
val name = jsonObj.get("A").toString
println(name)
*/

//将其写成函数形式

//有5种字段@timestamp,message,offset,source,source_id,source_ip,type;其中source_ip和record.key是一样的
/*
val events = stream.filter(record => record.value.contains("nginx-access")).map(record => {val jsonParser = new JSONParser()
  val jsonObj: JSONObject = jsonParser.parse(record.value).asInstanceOf[JSONObject]
  val name = jsonObj.get("message").toString
  name}).print(2)
  */

//val events = stream.filter(record => record.value.contains("nginx-access")).map(record => record.value()).print(2)

//显示message:..........格式（nginx-access类型日志）
/*
val events = stream.filter(record => record.value.contains("nginx-access")).map(record => record.value.split(",")(1).split("\":\"")(1)).print(1)
val events = stream.filter(record => record.value.contains("nginx-access")).map(record => pattern.findAllIn(record.value()).mkString(","))
 */

//查看变量类型
//stream.map(record => record.value.getClass.getSimpleName).print(4)
