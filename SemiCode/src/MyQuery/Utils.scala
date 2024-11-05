package MyQuery

import MyQuery.bgpProcess.subQueryComponent
import MyQuery.filterTranslator.ExprTranslator
import MyQuery.nodeClass.BGP_Node
import config.SparkConfiguration
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.expr.Expr
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{broadcast, col, explode_outer, udf}
import org.apache.spark.sql.types.{ArrayType, StructField}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.util.sketch.BloomFilter

import java.util
import java.util.regex.Pattern
import scala.collection.JavaConversions._
import scala.collection.mutable
/**
 * Created by jiguangxi on 2021/5/13 上午10:49
 *
 * changed by hongshen on 2022/08/24 14:19
 * todo: 封装一些工具方法
 */
object Utils {
  // DFchoose样例类:  一个表的相关信息: 结果表dataFrame,数据表元组数,扩展能力,公共变量,索引位置
  case class DFchoose(df: DataFrame, ts: Long, ev: Double, shareVars: util.HashSet[String], i:Int)

  var StoragePath = ""
  var resultPath=""
  var bgp_node: util.ArrayList[BGP_Node] = null                       // BGP节点数组
  var qn=""
  val translator:ExprTranslator=new ExprTranslator(null)  // filter条件转换器
  var varBfMap: mutable.HashMap[String, BloomFilter] = null          // 变量及其对应Bloom Filter的映射

  var sparkConfig: SparkConfiguration = null
  var sc: SparkContext = null
  var sqlCtx: SQLContext = null
  var pIndexSchema: scala.collection.Map[String, Array[String]] = null  // 谓词->模式索引
  var dataPatternTableSize: scala.collection.Map[String, Long] = null   // 数据表->表元组数
  var preSval: util.HashMap[String,Integer] = null                      // 每个谓词对应主语取值数
  var preOval: util.HashMap[String,Integer] = null                      // 每个谓词对应宾语取值数
  // 每个谓词的选择性(扩展能力?  每个谓词对应的宾语实体数量)   没有用到的
  var preSelector: util.HashMap[String,Double] = null

  var costOpt=false
  var bfOpt=false

  val pattern = Pattern.compile("[^a-zA-Z0-9]")
  val GREATER_THAN = " > "
  val GREATER_THAN_OR_EQUAL = " >= "
  val LESS_THAN = " < "
  val LESS_THAN_OR_EQUAL = " <= "
  val EQUALS = " = "
  val NOT_EQUALS = " != "
  val LOGICAL_AND = " AND "
  val LOGICAL_OR = " OR "
  val LOGICAL_NOT = "NOT "
  val BOUND = " is not NULL"
  val NOT_BOUND = " is NULL"
  val NO_SUPPORT = "#noSupport"


  val ADD = "+"
  val SUBTRACT = "-"
  val LIKE = " LIKE "
  val LANG_MATCHES = " LIKE "

  def init(args: Array[String]): Unit ={
    sparkConfig = new SparkConfiguration()
    sc = sparkConfig.getContext
    sqlCtx = sparkConfig.getSqlContext(sc)
    StoragePath = args(0)
    pIndexSchema = sc.objectFile[(String, Array[String])](args(0) + "statistic/pMapSchemaObjectFile").collectAsMap()
    dataPatternTableSize = sc.objectFile[(String, Long)](args(0) + "statistic/DataPatternTableSizeObjectFile").collectAsMap()
    preSval=new util.HashMap[String,Integer](sc.objectFile[(String, Integer)](args(0) + "statistic/subNumPreObjectFile").collectAsMap())
    preOval=new util.HashMap[String,Integer](sc.objectFile[(String, Integer)](args(0) + "statistic/objNumPreObjectFile").collectAsMap())
    preSelector = new util.HashMap[String,Double](sc.objectFile[(String, Double)](args(0) + "statistic/preSelectorObjectFile").collectAsMap())
    if(args.length>3)
      resultPath=args(3)
    costOpt=false
    bfOpt=false
    bgp_node= null
    varBfMap= null
    qn=""
  }

  //  todo: 对查询结果进行映射
  def resultMapping(finalR:DataFrame,qn:String): Unit ={
    // 1. 将所有列展开 并对longtype进行字典映射
    var dd=explodeJoinColumnByJoinVariable(finalR,new util.HashSet[String](finalR.columns.toList))

    println("展开后的元组数   "+dd.count())
    val colName2=dd.columns.sorted
    dd= dd.select(colName2.head, colName2.tail: _*)
    val csv_name="file://"+resultPath+qn.split("\\.")(0)+".csv"
//    dd.coalesce(1).write.format("csv").option("header",true).mode(SaveMode.Overwrite).save(csv_name)

//    dd.show()
//    val res = dd.select("_v0").collect().toSeq
//    res.foreach(println)

    println("处理完毕")
//    //    var dd=finalR
//    // 对列名进行排序
//    val colName2=dd.columns.sorted
//    dd= dd.select(colName2.head, colName2.tail: _*)
//
//    val csv_name=RunDriver.resultPath+qn.split("\\.")(0)+".csv"
////    val csv_name="/home/jiguangxi/Desktop/RBCresult/"+qn.split("\\.")(0)+".csv"
//    println(dd.count()+"   "+csv_name)
//    val csv_writer=new PrintWriter(new File(csv_name))
//    var head_string = ""
//    for(x<-dd.columns)
//      head_string+=","+x
//    csv_writer.write(head_string.substring(1)+"\n")
//    dd.collect().foreach(line=>csv_writer.write(line.toString().drop(1).dropRight(1)+"\n"))
//    csv_writer.close()
  }

  //   todo: 将公共变量对应的列进行扩展/展开为多行
  def explodeJoinColumnByJoinVariable(table: DataFrame, joinVariables: util.HashSet[String]): DataFrame = {
    val columnsToExplode = table.schema.fields.collect {
      case StructField(name, ArrayType(_, true), _, _) => name
    }.filter(joinVariables.contains(_))
    if (columnsToExplode.isEmpty) table
    else {
      val result = columnsToExplode.foldLeft(table) { (dataFrame, arrayCol) =>
        dataFrame.withColumn(arrayCol, explode_outer(col(arrayCol)))
      }
      result
    }
  }

  //   给定一个DataFrame表与一个子查询， 根据连接列进行扩展
  def explodeJoinColumn(table:DataFrame,joinvariables: util.List[String]):DataFrame={
    if(table==null)
      return null
    val columnsToExplode=table.schema.fields.collect{
      case StructField(name,ArrayType(_,true),_,_)=>name
    }.filter(joinvariables.contains(_))
    if(columnsToExplode.isEmpty) table           // 公共变量都是单值类型的，直接返回该表就行
    else {// 公共信息变量  多个字符串   是多值类型的？   需要对其进行展开？
      val result=columnsToExplode.foldLeft(table) { (dataFrame, arrayCol) =>
        dataFrame.withColumn(arrayCol, explode_outer(col(arrayCol)))}
      result
    }
  }


  //  todo: 对两个DataFrame进行内连接，获得中间结果DataFrame  子查询，DF1, DF2
  def innerJoin(subQuery: subQueryComponent, table: DataFrame, intermediateResult: DataFrame): DataFrame = {
    if(intermediateResult==null) table
    else{
      val joinVariables = subQuery.getJoinVariables
      var intermediateResultNew = explodeJoinColumnByJoinVariable(intermediateResult, new util.HashSet[String](joinVariables))
      if (joinVariables.size() != 0) {
        intermediateResultNew.join(table, joinVariables, "inner")
      } else {
        intermediateResultNew.join(table)
      }
    }
  }

  //  对两个DataFrame进行内连接，获得中间结果DataFrame  子查询，DF1, DF2
  def innerJoin(joinVariables:util.List[String], table:DataFrame, intermediateResult:DataFrame):DataFrame={
    if(intermediateResult==null)
      return table
    if(joinVariables.size()!=0)
      intermediateResult.join(explodeJoinColumn(table,joinVariables),joinVariables.toList,"inner")
    else table
  }

  //添加一个左半连接的方法， （1）半连接表也要炸开 （2）连接子表也要炸开
  def semiJoin(joinVariables:util.List[String], table:DataFrame, semiDF:DataFrame): DataFrame = {
    println("zai zheli zhix semi join")
    if(semiDF == null)
      return table
    if(joinVariables.size() != 0) {
      println("zai zheli zhix semi join &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
//      explodeJoinColumn(table, joinVariables).join(broadcast(explodeJoinColumn(semiDF, joinVariables)) , joinVariables.toList, joinType = "leftsemi")
      explodeJoinColumn(table, joinVariables).join(explodeJoinColumn(semiDF, joinVariables) , joinVariables.toList, joinType = "leftsemi")
    }
    else table
  }

  // TODO: 给定一个WDPT的BGP节点 建立对应的SPARQL SELECT查询
  def constructSPARQL(root: BGP_Node): String = {
    var select_statment: String = "SELECT\t"
    // 这里select出的,应该既包含必选变量,又包含可选变量
    for (x <- root.necessaryVarSet)
      select_statment += x + "\t"
//    for (x <- root.optionVarSet)
//      select_statment += x + "\t"
    val where_statment: String = getWhere(root)
    val query = select_statment + "WHERE\t{\n" + where_statment + "}"
    println("这里是一个查询"+query)
    query
  }

  // TODO: 获得一个查询中的常量约束
  def getWhere(opBGP1: BGP_Node): String = {
    var where_statment: String = ""
    for (tp <- opBGP1.getBGPTPs()) {
      var sub = tp.getSubject.toString
      var obj = tp.getObject.toString
      if (sub.startsWith("http://"))
        sub = "<" + sub + ">"
      if (obj.startsWith("http://"))
        obj = "<" + obj + ">"
      where_statment += sub + "\t<" + tp.getPredicate.toString() + ">\t" + obj + "\t.\n"
    }
    where_statment
  }

  def getVarsOfTps(tps:util.List[Triple], rep: String):util.HashSet[String]={
    val vars_set: util.HashSet[String] = new util.HashSet[String] // 变量集
    for (tp <- tps) { // 遍历每个三元组模式
      val sub = tp.getSubject.toString.replace("?", rep)
      val obj = tp.getObject.toString.replace("?", rep)
      val pre = pattern.matcher(tp.getPredicate.toString).replaceAll("").toLowerCase()
      if (sub.startsWith(rep))
        vars_set.add(sub)
      if (obj.startsWith(rep))
        vars_set.add(obj)
    }
    vars_set
  }



  // todo: 给定一个Expr条件, 获得其涉及的变量集合
  def getVarsExpr(ep: Expr): util.HashSet[String] = {
    val vv = new util.HashSet[String]
    import scala.collection.JavaConversions._
    for (v <- ep.getVarsMentioned) {
      vv.add(v.toString.replace("?", "_"))
    }
    vv
  }

  def getConsExpr(ep: Expr): util.HashSet[String] = {
    val vv = new util.HashSet[String]
    import scala.collection.JavaConversions._
    for (v <- ep.getFunction.getArgs) {
      if(v.isConstant)
        vv.add(v.getConstant.toString)
    }
    vv
  }

  // todo: 数组内进行数据过滤的一些条件
  // (1) 变量与固定值的比较   都可以使用模板来处理
  val afGraterValue=udf((x:Seq[String], t:String)=>{    // 大于定值
    if(x==null||t==null) null
    else {val tt=x.filter(a => a>t)
      if(tt.size>0) tt else null}
  })

  val afGraterEqualValue=udf((x:Seq[String], t:String)=>{    // 大于等于定值
    if(x==null||t==null) null
    else {val tt=x.filter(a => a>=t)
      if(tt.size>0) tt else null}
  })

  val afEqualValue=udf((x:Seq[String], t:String)=>{   // 等于定值
    if(x==null||t==null) null
    else {val tt=x.filter(a => a==t)
      if(tt.size>0) tt else null}
  })

  val afLessEqualValue=udf((x:Seq[String], t:String)=>{ // 小于等于定值
    if(x==null||t==null) null
    else {val tt=x.filter(a => a<=t)
      if(tt.size>0) tt else null}
  })

  val afLessValue=udf((x:Seq[String], t:String)=>{    // 小于定值
    if(x==null||t==null) null
    else {val tt=x.filter(a => a<t)
      if(tt.size>0) tt else null}
  })

  val afNotEqualValue=udf((x:Seq[String], t:String)=>{    // 不等于定值
    if(x==null||t==null) null
    else {val tt=x.filter(a => a!=t)
      if(tt.size>0) tt else null}
  })

  // 列字段相等, 使用该函数两次   ?x=?y
  val afEqualVarAA=udf((x:Seq[String], t:Seq[String])=>{
    if(x==null||t==null) null
    else {val tt=x.intersect(t)
      if(tt.size>0) tt else null}
  })

  // 列字段相等, 使用该函数两次   ?x=?y
  val afEqualVarAS=udf((x:Seq[String], t:String)=>{
    if(x==null||t==null) null
    else {val tt=x.filter(a=>a==t)
      if(tt.size>0) tt else null}
  })

}
