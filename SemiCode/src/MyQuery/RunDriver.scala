package MyQuery

import MyQuery.RBC_MyQuery.{MyOptional, NodeMerge}
import MyQuery.Utils._
import MyQuery.buildCWDPT.QueryParse
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import java.io.File
import scala.collection.JavaConversions._
/**
 * Created by jiguangxi on 2021/5/13 下午2:14
 * 参数: HDFS数据存储目录, 查询文件路径, 优化类型, 结果存储路径
 */
object RunDriver {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // 初始化Spark环境与相关统计信息
    Utils.init(args)
    val typee = args(2)
    val queryPath=args(1)
    if(!queryPath.endsWith("/"))
      executorQuery(typee,queryPath)
    else{
      val f:File=new File(queryPath)
      for(qName <- f.list)
        executorQuery(typee,queryPath+qName)
    }
  }


  def executorQuery(typee:String,query:String): Unit ={
    val executeStartTime = System.currentTimeMillis()
    var finalR:DataFrame=null
    // 1. 查询解析, 获得对应的CWDPT树(更新至bgp_node中)
    val wdFlag = QueryParse.parseQuery(query)
    if(wdFlag==false) {
      println("不是well-designed查询, 拒绝执行")
      return
    }

    // 2. 查询执行, 各种优化技术尝试解耦?
    if(typee.contains("R"))
      NodeMerge.queryMerge()    // 基于规则的节点合并优化
    if(typee.contains("B"))
      Utils.bfOpt=true
    if(typee.contains("C"))
      Utils.costOpt=true


    var semiDF:DataFrame = null
    var pairOne = ""
    var pairTwo = ""
    var subTableName = "NullTableName"
//    finalR= MyOptional.upDown_WDPT_Executor(bgp_node(0), varBfMap)
    finalR = MyOptional.upDown_WDPT_Executor(bgp_node(0), semiDF, pairOne, pairTwo, subTableName)
//    finalR.cache()
    val qTime = System.currentTimeMillis() - executeStartTime
    println("before " + qTime + " ms")
    //  todo: (四)结果映射,将Id转换为字符串
    val startTime1 = System.currentTimeMillis()
    println("==============="+qn+"  结果数:"+finalR.count()+"        查询时间"+qTime+"ms")
    val endTime1 = System.currentTimeMillis()
    println(endTime1 - startTime1 + " ms" + " 纯count() 的 time")
    Utils.resultMapping(finalR,qn) // 这部分属于查询之后的结果的处理,怎么的形式存在文件中,所以并算查询的时间范围内
    println("-------------------------------"+qn+"执行完毕 Semi-----------------------------------------")
  }
}