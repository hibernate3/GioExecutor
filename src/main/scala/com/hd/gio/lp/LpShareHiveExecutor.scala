package com.hd.gio.lp

import com.alibaba.fastjson.JSONObject
import com.hd.gio.util.DBUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object LpShareHiveExecutor {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder().appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    sqlContext.sparkContext.setLogLevel("WARN")

    buildingRecCount(sqlContext)
  }

  //楼盘推荐处理
  def buildingRecCount(sparkSession: SparkSession): Unit = {
    val sql: String = "SELECT building_rb_id,count(1) AS rec_count from hdb.ods_hdb_mysql_client_building_relation GROUP BY building_rb_id"

    val recResult: DataFrame = sparkSession.sql(sql)

    //将结果写入到Mysql
    val dbUtil = new DBUtil()
    dbUtil.writeToMysql("10.101.40.216", 3306, "bigdata_result", "hdb_user", "hdb123456", "lp_recommend_result", recResult, 0)
  }
}
