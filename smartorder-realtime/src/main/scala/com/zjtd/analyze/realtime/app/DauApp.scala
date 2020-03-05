package com.zjtd.analyze.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.zjtd.analyze.common.GmallConstants
import com.zjtd.analyze.realtime.bean.StartUpLog
import com.zjtd.analyze.realtime.utils.{MykafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //1 消费kafka
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    /* inputDstream.foreachRDD{rdd=>
       println(rdd.map(_.value()).collect().mkString("\n"))
     }*/

    //2 数据流 转换 结构变成case class 补充两个时间字段
    val startUplogDstream: DStream[StartUpLog] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      val formator = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHour: String = formator.format(new Date(startUpLog.ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      startUpLog
    }

    startUplogDstream.cache()

    // 3   利用用户清单进行过滤 去重  只保留清单中不存在的用户访问记录
    //driver
    val filteredDstream: DStream[StartUpLog] = startUplogDstream.transform { rdd =>
      //driver 每批次执行一次

      val jedis: Jedis = RedisUtil.getJedisClient
      val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val dauKey = "dau:" + dateString
      val dauMidSet: util.Set[String] = jedis.smembers(dauKey)
      jedis.close()

      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
      println("过滤前:" + rdd.count())

      val filteredRdd: RDD[StartUpLog] = rdd.filter { startuplog =>
        //executer
        val dauMidSet: util.Set[String] = dauMidBC.value
        val flag: Boolean = dauMidSet.contains(startuplog.mid)
        !flag
      }
      println("过滤后:" + filteredRdd.count())
      filteredRdd
    }

    // 4 批次内进行去重：：按照mid 进行分组，每组取第一个值
    val groupbyMidDstream: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val distictDstream: DStream[StartUpLog] = groupbyMidDstream.flatMap { case (mid, startupLogItr) =>
      val top1List: List[StartUpLog] = startupLogItr.toList.sortWith { (startup1, startup2) =>
        startup1.ts < startup2.ts
      }.take(1)
      top1List
    }

    // 5 保存今日访问过的用户(mid)清单   -->Redis    1 key类型 ： set    2 key ： dau:2019-xx-xx   3 value : mid
    startUplogDstream.foreachRDD{rdd=>
      //driver
      rdd.foreachPartition{startupItr=>
        val jedis: Jedis = RedisUtil.getJedisClient//executor
        for (startup <- startupItr) {
          //excutor 反复执行
          val dauKey="dau:"+startup.logDate
          jedis.sadd(dauKey,startup.mid)
          println(startup)
        }
        jedis.close()
      }
    }

    startUplogDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL2019_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("192.168.1.101,192.168.1.102,192.168.1.103:2181"))

    }


    ssc.start()
    ssc.awaitTermination()
  }

}
