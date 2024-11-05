package MyQuery.RBC_MyQuery

import MyQuery.Utils
import MyQuery.Utils.bgp_node
import MyQuery.nodeClass.{BGP_Node, Local_Node}
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.expr.Expr

import java.util
import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}
/**
 * Created by jiguangxi on 2021/11/30 下午10:45
 * 基于规则的节点合并类, 父子节点间合并与兄弟节点键合并
 *
 * changed by hongshen on 2022/08/23 下午21:21
 */
object NodeMerge {

  // 基于规则的节点合并改写
  def  queryMerge(): Unit ={
    // 1 父子节点之间合并
    for(fa <- bgp_node) {
      for(ch <- fa.children)
        fatherSonMerge(fa, ch)
    }
//    unvalidNode(bgp_node)   // 删除无效节点 并更新节点间父子关系
    println("父子节点合并之后======")
    for (node <- bgp_node)
      println(node.toString)

    println("==================")
  }



//  // 移除无效节点(即合并后不存在一个三元组模式的节点)
//  def unvalidNode(nodes: util.ArrayList[BGP_Node]): Unit ={
//    val nodesBack=new util.ArrayList[BGP_Node](nodes)
//    for(node<-nodesBack){
//      if(node.getBGPTPs.toList.size==0){
//        var chNewFather=node.father
//        chNewFather.children.remove(node)
//        for(ch<-node.children){
//          ch.father=chNewFather
//          chNewFather.addChildren(ch)
//        }
//        nodes.remove(node)
//      }
//    }
//  }
//
//  def noConstObject(tps: util.ArrayList[Triple]):Boolean={
//    for(tp<-tps){
//      if(!(tp.getObject.toString().startsWith("?")||tp.getObject.toString().startsWith("_")))
//        return false
//    }
//    return true
//  }

//  def fatherSonMerge2(fa:BGP_Node, ch:BGP_Node):Unit={
//    val newVarSet = new util.HashSet[String](ch.necessaryVarSet) //当前孩子节点的所有变量
//    for(supernode <- ch.getAllSuperNode) {
//      val sub = supernode.subject;
//
//      val superNodeFa = fa.findSuperNodeBySubject(sub) //usr same sub to find superNode
//      val superNodeFaUp = fa.findSuperNodeUpBySubject(sub)
//      if(superNodeFa != null) ch.superNodesUp.add(superNodeFa)
//      if(superNodeFaUp != null) ch.superNodesUp.add(superNodeFaUp)
//
//      val lnchs = new util.ArrayList[Local_Node](supernode.localnodes);
//      //先不管变量
//
//      for(ln <- lnchs){ //遍历超节点的局部节点
//        //这里的寻早相同schema的，是对父节点中的所有supernodes的所有localnodes里面寻找相同的schema返回local node。
//        var lnfa = fa.findLocalNodeBySchema(ln.subject, ln.table) //这个是父节点里面的local node。
//        //  todo:  论文中这里必须ch节点全部为单变量原子FILTER时才可以进行合并   条件条件 ch.singleAutoFilter()==true
//        if(lnfa != null){
//          //  (1) 合并和维护
//          ln.getTPs.addAll(lnfa.getTPs)  //添加之后，必选变量就改变了
//          ln.tpsUp.addAll(lnfa.getTPs) //移动下来的三元组额外存一份，单独维护
//          ln.varSetUp = Utils.getVarsOfTps(lnfa.getTPs, "_") //移动下来的三元组中的变量
//          //          ln.varSetUp.remove(ln.subject) // 去除subject,只留下父节点中带来的新变量,这些字段后续不会成为子查询中的列,即要删除的列
//          ln.varSetUp.removeAll(newVarSet) // 保险起见,删除所有孩子节点中的变量而不只是删subject,虽然事实上,公共变量应该就这一个
//        }
//
//        // (2) filter部分维护
//
//        //我这边应该是 ch中的ln添加合并下来的父节点的ln的表达式，是添加到ln单独的一个属性中维护
//        for(ep <- lnfa.exprs) {
//          ln.exprsUp.add(ep) // 是local node 的合并下来的filter表达式集合，统一添加到这个属性中
//        }
//        //我只需要考虑移动下来的local node的表达式就行。不考虑全局的，认为全局的涉及别的local node中的变量,所以移动下来也不会有大帮助,后续在说.
//      }
//    }//for 循环遍历孩子节点的所有超节点
//  }

  def fatherSonMerge(fa:BGP_Node, ch:BGP_Node):Unit={
    val newVarSet = new util.HashSet[String](ch.necessaryVarSet) //当前孩子节点的所有变量
    //遍历孩子节点本部的superNode
    for(supernode <- ch.getAllSuperNode) {
      val sub = supernode.subject;
      val lnchs = new util.ArrayList[Local_Node](supernode.localnodes);
      //先不管变量

      for(ln <- lnchs){ //遍历每个superNode的每个local Node
        //从父节点的本部的superNode中找相同schema 的local node
        val lnfa = fa.findLocalNodeBySchema(ln.subject, ln.table)
        var flag = 0
        if(lnfa != null){
          flag = 1
          //  (1) 合并和维护
          val tripleUp = scala.collection.mutable.Set[Triple]()
          for(tripleListTmp <- lnfa.getTPs) {
            println(" from father node's triple is " + flag + " " + tripleListTmp)
            tripleUp.add(tripleListTmp)
          }
          //用set去个三元组的重,反正最终相同local node的三元组数量也不多,代价可接受
          for(tripleTmpSet <- tripleUp) {
            ln.tps.add(tripleTmpSet)
            ln.tpsUp.add(tripleTmpSet)
          }

//          ln.getTPs.addAll(lnfa.getTPs)  //添加之后，必选变量就改变了
//          ln.tpsUp.addAll(lnfa.getTPs) //移动下来的三元组额外存一份，单独维护
          ln.varSetUp = Utils.getVarsOfTps(lnfa.getTPs.toList, "_") //移动下来的三元组中的变量
//          ln.varSetUp.remove(ln.subject) // 去除subject,只留下父节点中带来的新变量,这些字段后续不会成为子查询中的列,即要删除的列
          ln.varSetUp.removeAll(newVarSet) // 保险起见,删除所有孩子节点中的变量而不只是删subject,虽然事实上,公共变量应该就这一个
//          (2) filter部分维护
          for(ep <- lnfa.exprs) {  //应该并不需要去重
            ln.exprsUp.add(ep) // 是local node 的合并下来的filter表达式集合，统一添加到这个属性中
          }
        }
        else { //对于当前孩子节点的superNode[限制了本部和up中相同subject], 本部中没有,才需要从up的superNode中去判断local node,因为本部有,那么up中的同subject的superNode的三元组之前已经合并到本部中
          println(" from father benbu node has no same schmea " + flag )
          for (superNodeUp <- fa.superNodesUp) {
            import scala.collection.JavaConversions._
            for (localNodeUp <- superNodeUp.localnodes) {
              if (localNodeUp.sameSchema(ln.subject, ln.table)) {
                val tripleUp2 = scala.collection.mutable.Set[Triple]()
                for(tripleListTmp <- localNodeUp.getTPs) {
                  tripleUp2.add(tripleListTmp)
                }
                //用set去个三元组的重,反正最终相同local node的三元组数量也不多,代价可接受
                for(tripleTmpSet <- tripleUp2) {
                  ln.tps.add(tripleTmpSet)
                  ln.tpsUp.add(tripleTmpSet)
                }

                ln.varSetUp = Utils.getVarsOfTps(localNodeUp.getTPs.toList, "_") //移动下来的三元组中的变量
                ln.varSetUp.removeAll(newVarSet) // 保险起见,删除所有孩子节点中的变量而不只是删subject,虽然事实上,公共变量应该就这一个

                for(ep <- localNodeUp.exprs) {  //应该并不需要去重
                  ln.exprsUp.add(ep) // 是local node 的合并下来的filter表达式集合，统一添加到这个属性中
                }
                ln.exprsUp.removeAll(ln.exprs) //还是去重一下,删除自己本身的filter表达式,移动下来的只有新增加的filter表达

              }

            } //for localNode
          } // for superNode
        }// else
        //我只需要考虑移动下来的local node的表达式就行。不考虑全局的，认为全局的涉及别的local node中的变量,所以移动下来也不会有大帮助,后续在说.
      } // for merge local node
      //merge father's superNode to child's superNodeUp

      breakable {
        //    (1) 遍历父节点本部的supernode,本部(一个bgp节点)只会有唯一的一个superNode
        for (superNodeSelf <- fa.superNodes) {
          val subFaSelf = superNodeSelf.subject
          if (subFaSelf.equals(sub)) {
            ch.superNodesUp.add(superNodeSelf)
            break
          }
        }
      }
//        (2) 遍历父节点superNodeUp
      for(superNodeUp <- fa.superNodesUp) {
        val subFaUp = superNodeUp.subject
        if(subFaUp.equals(sub)) {
          ch.superNodesUp.add(superNodeUp)
        }
      }

    }//for 循环遍历孩子节点的所有超节点

  }

//   加上一点,对FIlter条件进行判断
//  def fatherSonMerge(fa:BGP_Node, ch:BGP_Node):Unit={
////    if(!(ch.isLeaf==true&&ch.isSingleSuperNode()==true)) //我的合并不需要满足这些条件
////      return ;
//    val sub=ch.getSingleSuperNode.subject;
//    val lnchs=new util.ArrayList[Local_Node](ch.getSingleSuperNode.localnodes)
//    val newVarSet=new util.HashSet[String](ch.necessaryVarSet)    // 父亲节点中没有, 孩子节点新引入的变量集合
//    newVarSet.removeAll(fa.necessaryVarSet)
//    for(ln<-lnchs){
//      var lnfa=fa.findLocalNodeBySchema(ln.subject,ln.table)
//      //  todo:  论文中这里必须ch节点全部为单变量原子FILTER时才可以进行合并   条件条件 ch.singleAutoFilter()==true
//      if(ln.noConstObject()==true&&lnfa!=null){
//        //  (1) 可选分组维护
//        lnfa.getTPs.addAll(ln.getTPs)
//        val optVars=new util.HashSet[String](ch.getBGPVars())
//        optVars.remove(sub)
//        if(optVars.size()>1){
//          fa.optionVarSet.addAll(optVars)
//          fa.optionVarGroup.add(optVars)
//        }
//        ch.deleteUnVaildNode(ln)
//
//        // (2) filter部分维护
//        //todo: 为firstNode添加filterVarGroup  首先是本local node对应的局部filter, 然后是当前BGP节点对应的全局filter条件
//        for(ep<-ln.exprs){
//          fa.filterVarGroup.add(ep)
//          fa.filterVarGroupMapping.put(ep,newVarSet)
//        }
//        val removeGBExpres:util.ArrayList[Expr]=new util.ArrayList[Expr]()
//        for(ep<-ch.gbExprs){                  //  添加全局性的条件
//          fa.filterVarGroup.add(ep)
//          fa.filterVarGroupMapping.put(ep,newVarSet)
//          removeGBExpres.add(ep)
//        }
//        ch.gbExprs.removeAll(removeGBExpres)
//      }
//    }
//    ch.necessaryVarSet=ch.getBGPVars()
//    if(ch.necessaryVarSet.size()==0)
//      ch.live==false
//  }

//  def brotherMerge(ch1:BGP_Node,ch2:BGP_Node): Unit ={
//    if(!(ch1.isLeaf && ch2.isLeaf))
//      return
//    if(!(ch1.isSingleLocalNode&&ch2.isSingleLocalNode))
//      return
//    val ln1=ch1.getSingleLocalNode
//    val ln2=ch2.getSingleLocalNode
//    val sub=ln1.subject
//
//    val optVar1=new util.HashSet[String](ch1.getBGPVars)
//    optVar1.remove(sub)
//
//    val optVar2=new util.HashSet[String](ch2.getBGPVars)
//    optVar2.remove(sub)
//
//    if(ln1.sameSchema(ln2.subject,ln2.table)==true&&ln1.noConstObject()==true&&ln2.noConstObject()){
//      if(ch1.optionVarGroup.size()==0){
//        if(optVar1.size()>1)
//          ch1.optionVarGroup.add(optVar1)
//      }
//      if(ch2.optionVarGroup.size()==0){
//        if(optVar2.size()>1)
//          ch1.optionVarGroup.add(optVar2)
//      }else{
//        ch1.optionVarGroup.addAll(ch2.optionVarGroup)
//      }
//      ln1.getTPs.addAll(ln2.getTPs)
//      ch2.deleteUnVaildNode(ln2)
//      ch1.necessaryVarSet=new util.HashSet[String]()
//      ch1.necessaryVarSet.add(sub)
//
//
//      // todo: 要对filter部分进行处理   注意合并的要求:两个叶子节点,相同主语并且唯一相同local node
//      // ch1还是一个原子节点  并且存在条件,则先加入filterVarGroup中,  如果ch1为组合节点,则filter条件不再考虑
//      if(ch1.filterVarGroup.size()==0&&ln1.exprs.size()!=0){
//        for(ep<-ln1.exprs){
//          ch1.filterVarGroup.add(ep)
//          ch1.filterVarGroupMapping.put(ep,optVar1)
//        }
//        ln1.exprs.clear()
//      }
//      if(ch2.filterVarGroup.size()==0&&ln2.exprs.size()!=0){
//        for(ep<-ln2.exprs){
//          ch1.filterVarGroup.add(ep)
//          ch1.filterVarGroupMapping.put(ep,optVar2)
//        }
//        ln2.exprs.clear()
//      }else{
//        for(filterVars<-ch2.filterVarGroup){
//          ch1.filterVarGroup.add(filterVars)
//          ch1.filterVarGroupMapping.put(filterVars,ch2.filterVarGroupMapping.get(filterVars))
//        }
//      }
//
//      ch2.live=false
//    }
//  }
}
