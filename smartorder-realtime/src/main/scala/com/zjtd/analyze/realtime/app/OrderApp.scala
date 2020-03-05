package com.zjtd.analyze.realtime.app

import com.alibaba.fastjson.JSON
import com.zjtd.analyze.common.GmallConstants
import com.zjtd.analyze.realtime.bean.OrderInfo
import com.zjtd.analyze.realtime.utils.MykafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.phoenix.spark._
object OrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER,ssc)

    //    inputDstream.map(_.value()).foreachRDD(rdd=>
    //      println(rdd.collect().mkString("\n"))
    //    )
    //

    val orderInfoDstrearm: DStream[OrderInfo] = inputDstream.map {
      _.value()
    }.map { orderJson =>
      val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
      //日期

      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      val timeArr: Array[String] = createTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      // 收件人 电话 脱敏
      orderInfo.consignee_tel = "*******" + orderInfo.consignee_tel.splitAt(7)._2
      orderInfo
    }


    orderInfoDstrearm.foreachRDD { rdd =>


      val configuration = new Configuration()
      println(rdd.collect().mkString("\n"))

      rdd.saveToPhoenix("GMALL_ORDER_INFO", Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), configuration, Some("192.168.1.101,192.168.1.102,192.168.1.103:2181"))

    }

    ssc.start()
    ssc.awaitTermination()


  }

}
