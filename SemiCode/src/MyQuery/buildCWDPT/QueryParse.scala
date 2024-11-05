package MyQuery.buildCWDPT

import MyQuery.Utils._
import MyQuery.nodeClass.BGP_Node
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.expr.Expr
import org.apache.spark.util.sketch.BloomFilter

import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by jiguangxi on 2022/4/1 下午2:10
 * SPARQL查询解析类   输入SPARQL查询, 输出为对应的CWDPT树
 * 查询解析逻辑:  SPARQL查询-->Jena ARQ代数树-->优化后代数树-->CWDPT树-->转换规则-->验证是否为well-designed查询
 *              -->细粒度模式树-->FILTER条件优化
 */
object QueryParse {

  def parseQuery(fileName:String): Boolean ={
//    varBfMap=new mutable.HashMap[String, BloomFilter]()
    // 1. 获得查询文件名
    qn=fileName.split("/").last
    println("现在处理的查询名  "+qn)
    // 2. 读取查询,构建对应的非冗余CWDPT树形式
    bgp_node = RuleOptimization.ruleOptimizationQuery(fileName)
    // 3. 基于CWDPT树,判断查询是否为well-designed查询
    if(!(WDTest.isUwd(bgp_node)==true && WDTest.filterCheck(bgp_node) == true))
      return false
    // 4. 构造Super Node与Local Node信息
    for(node<-bgp_node) {
      // 变量集,  主语变量集,  (subject, tab list),  (主语, (数据表, 三元组模式集)),  (主语, (数据表, 布尔值))
      val vars = getVarsOfBGP(node.initTPs, "_")
      node.initBGPSL(vars._1, vars._2, vars._3,vars._4,vars._5)
    }

    //  5. filter优化,组合型FILTER拆分,  BGP间迁移和约束性处理 ,  子节点对应  共四种
    filterOptimization(bgp_node(0))
    return true
  }


  // 对Filter进行优化 1组合型Expr拆分;  2 BGP间Expr复制 3 Expr下推 4  FILTER约束性
  def filterOptimization(root:BGP_Node):Unit={
    root.splitFilter()
    for(ch:BGP_Node<-root.children){  // 遍历所有孩子节点, 遍历所有Expr条件, 满足变量集包含关系,则可以将expr下推到孩子节点
      for(ep:Expr<-root.allExprs){
        if(ch.necessaryVarSet.containsAll(root.epVars.get(ep))){
          ch.allExprs.add(ep)
          ch.epVars.put(ep, root.epVars.get(ep))
        }
      }
      filterOptimization(ch)
    }
    root.sortExpr()
    root.exprToLN()
  }

  // 获得一个查询BGP中的所有变量,  所有主语,  每个主语对应的数据表
  // 输入 BGP模式;   替换符
  def getVarsOfBGP(tps: util.ArrayList[Triple], rep: String): (util.HashSet[String], util.HashSet[String], util.HashMap[String,
    util.HashSet[String]], util.HashMap[String,util.HashMap[String,util.ArrayList[Triple]]],
    util.HashMap[String, util.HashMap[String, java.lang.Boolean]]) = {
    // 全部变量集合
    val vars_set: util.HashSet[String] = new util.HashSet[String]
    // 主语变量集合
    val subSet: util.HashSet[String] = new util.HashSet[String]
    // 每个主语以及对应的数据表列表  (subject, tab list)
    val varsTableN: util.HashMap[String, util.HashSet[String]] = new util.HashMap[String, util.HashSet[String]]()
    // (主语, (数据表, 三元组模式集))  建模Local Node
    val ln2Triples:util.HashMap[String,util.HashMap[String,util.ArrayList[Triple]]]=new util.HashMap[String,util.HashMap[String,util.ArrayList[Triple]]]
    // (主语, (数据表, 布尔值))  Local Node是否可用(未被合并到其他节点中)
    val ln2Avi:util.HashMap[String, util.HashMap[String, java.lang.Boolean]]=new util.HashMap[String, util.HashMap[String, java.lang.Boolean]](); // todo: 某局部节点是否可用

    // 遍历所有三元组模式, 维护上面的信息
    for (tp <- tps) {
      // 首先获得主谓宾语   以及对应的数据表
      val sub = tp.getSubject.toString.replace("?", rep)
      val obj = tp.getObject.toString.replace("?", rep)
      var pre = pattern.matcher(tp.getPredicate.toString).replaceAll("").toLowerCase()
      if(pre.contains("file"))
        pre="<"+pre.substring(pre.lastIndexOf('/'),pre.length-1)+">"
      val tableName=pIndexSchema(pre)(0)

      subSet.add(sub)
      if (!varsTableN.containsKey(sub))
        varsTableN.put(sub, new util.HashSet[String]())
      varsTableN.get(sub).add(tableName)   // 主语以及对应的数据表列表

      //  ======= //
      if(!ln2Triples.containsKey(sub)) {
        ln2Triples.put(sub,new util.HashMap[String,util.ArrayList[Triple]])
        ln2Avi.put(sub,new util.HashMap[String,java.lang.Boolean]())
      }
      if(!ln2Triples.get(sub).containsKey(tableName)) {
        ln2Triples.get(sub).put(tableName,new util.ArrayList[Triple]())
        ln2Avi.get(sub).put(tableName,true)
      }
      ln2Triples.get(sub).get(tableName).add(tp)      // 主语＋表名　＝＝> 三元组列表  找到指定的Local Node

      // 变量集
      if (sub.startsWith(rep))
        vars_set.add(sub)
      if (obj.startsWith(rep))
        vars_set.add(obj)
    }
    (vars_set, subSet, varsTableN,ln2Triples,ln2Avi)
  }
}
