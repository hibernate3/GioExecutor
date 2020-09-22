package com.hd.gio.lp

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.common.serialization.StringDeserializer

case class SEvent(user_id: String, event_time: String)

object LpShareKafkaExecutor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("hdfs://HDBDC:8020/temp/GioExecutor/kafka_checkpoint/lp_share")

    val topics = Array("cdp_event_collect_trans")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata-prd-cdh-kafka-01:9092, bigdata-prd-cdh-kafka-02:9092, bigdata-prd-cdh-kafka-03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ge_lp_share",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val result = kafkaDStream.print()

    kafkaDStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //获取当前批次的RDD的偏移量
        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //拿出kafka中的数据
        val lines = kafkaRDD.map(_.value())

        //将lines字符串转换成json对象
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
