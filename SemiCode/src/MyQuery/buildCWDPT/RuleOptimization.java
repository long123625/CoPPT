package MyQuery.buildCWDPT;


import MyQuery.nodeClass.BGP_Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Stack;

/**
 * Created by jiguangxi on 2021/11/3 下午8:57
 * 解析SPARQL查询, 基于规则1~6进行简化,获得非冗余CWDPT树形式
 */
public class RuleOptimization  extends OpVisitorBase {

    public static Stack<Op> st=new Stack<Op>();
    public static ArrayList<BGP_Node> arr=new ArrayList<>();

    public static ArrayList<BGP_Node> ruleOptimizationQuery(String fileName){
        //  1. 读取查询并得到其代数树形式
        Query query= QueryFactory.read(fileName);
        Op opRoot = Algebra.compile(query);
        Op last = opRoot;
        System.out.println("原始代数树形式\n"+opRoot);
        //  2. 开始进行转换, 基于TransRuleOne()中规则,自下而上遍历opRoot代数树,并将转化后的Op加入到栈中.  规则1~3
        Transformer.transform(new TransRuleOne(), opRoot);
        Op nowOp = st.pop();                              // 本轮转化后的代数树形式
        //  反复执行转换,直至代数树不再发生变化
        while(!last.toString().equals(nowOp.toString())){
            last=nowOp;
            Transformer.transform(new TransRuleOne(), nowOp);
            nowOp=st.pop();
        }
        //  3. nowOp代数树中只剩下bgp, leftjoin与filter节点
        System.out.println("开始打印规则1~3优化后代数树");
        System.out.println(nowOp.toString());
        System.out.println("=======================");

        //  4. 递归访问nowOp, 构建对应的BGP_Node节点 并建立树  构建CWDPT树
        visitNode(nowOp,0,new ArrayList<OpFilter>());
        bgpEdges(arr);

//        // 5. 两次深搜遍历,去除冗余三元组和无效节点    规则4~6  这个规则对吗?
//        dfsVisitTP(arr.get(0),new HashSet<Triple>());
//        dfsVisitVars(arr.get(0),new HashSet<String>());
//        System.out.println("进一步优化后的CWDPT树");
//        for(int i=0;i<arr.size();i++){
//            if(arr.get(i).live==false)
//                arr.remove(arr.get(i));
//        }
        for(int i=0;i<arr.size();i++){
            if(arr.get(i).children.size()==0)
                arr.get(i).isLeaf=true;
        }
        System.out.println("=======================");
        return arr;
    }


    //  自上而下遍历每个节点,为opBGP建立对应的BGP节点
    public static void visitNode(Op op, int height, ArrayList<OpFilter> filters){
        if(op instanceof  OpLeftJoin){      // 对于一个LeftJion节点   左右处理
            OpLeftJoin leftjoin = (OpLeftJoin)op;
            //  左孩子  直接向下即可
            ArrayList<OpFilter> leftFilter = new ArrayList<>(filters);
            visitNode(leftjoin.getLeft(), height, leftFilter);
            //  右孩子  考虑将当前OpLeftJoin节点的条件加上
            ArrayList<OpFilter> rightFilters = new ArrayList<>();
            if(leftjoin.getExprs() != null)
                rightFilters.add(OpFilter.filterAlways(leftjoin.getExprs(), null));
            visitNode(leftjoin.getRight(), height + 1, rightFilters);
        }else if(op instanceof OpFilter){
            OpFilter opfilter = (OpFilter)op;
            ArrayList<OpFilter> tmpFilter = new ArrayList<>(filters);
            tmpFilter.add(opfilter);
            visitNode(opfilter.getSubOp(), height, tmpFilter);
        }else{
            addBGP((OpBGP)op, height, filters);
        }
    }

    // 给定一个BGP节点(可能有FILTER约束), 构建对应的CWDPT节点
    public static void addBGP(OpBGP op, int height, ArrayList<OpFilter> filter){
        arr.add(new BGP_Node(op, height, arr.size(), filter));
    }

    // 构建CWDPT树, 根据高度建立WDPT树,从后向前遍历,高度为i+1的BGP节点的父亲节点应该为前面第一个高度为i的节点
    public static void bgpEdges(ArrayList<BGP_Node> bgp_node){
        for(int i=bgp_node.size()-1;i>=0;i--){
            for(int j=i-1;j>=0;j--){
                if(bgp_node.get(j).height<bgp_node.get(i).height){
                    bgp_node.get(i).setFather((bgp_node.get(j)));
                    bgp_node.get(j).addChildren(bgp_node.get(i));
                    break;
                }
            }
        }
    }

    // 下面两个方法不先考虑了
    // 从根节点开始 深搜遍历,维护祖先节点的所有三元组,然后将冗余三元组去除
    public static void dfsVisitTP(BGP_Node root, HashSet<Triple> ancestorTriple){
        // 这里只是处理BGP三元组模式, 与SL无关的
        root.initTPs.removeAll(ancestorTriple);    // 在root节点中 移除祖先中已有的三元组模式
        ancestorTriple.addAll(root.initTPs);        // 将root节点的模式加入到祖先模式集合中
        ArrayList<BGP_Node> chidrenBack = new ArrayList<>(root.children);
        for(BGP_Node ch: chidrenBack)              // 递归处理所有孩子节点
            dfsVisitTP(ch, ancestorTriple);
        ancestorTriple.removeAll(root.initTPs);
        if(root.initTPs.size() == 0){ //如果当前节点处理之后,成为空节点
            root.live = false;
            root.father.children.remove(root);
            for(BGP_Node ch: root.children){ //更新当前节点
                if(ch.live== true){
                    ch.setFather(root.father);
                    root.father.addChildren(ch);
                }
            }
        }
    }

    // 深搜遍历,维护祖先节点的所有变量,将未引入新变量的冗余节点去除.
    public static void dfsVisitVars(BGP_Node root, HashSet<String> ancestorVar){
        HashSet<String> vars = new HashSet<>(root.getInitVars()); // 当前节点的变量集
        vars.removeAll(ancestorVar);                    // 与祖先节点 变量集  取差集       新引入的变量

        ancestorVar.addAll(vars);
        ArrayList<BGP_Node> chidrenBack = new ArrayList<>(root.children);
        for(BGP_Node ch: chidrenBack)
            dfsVisitVars(ch, ancestorVar);
        ancestorVar.removeAll(vars);
        if(vars.size() == 0){
            root.live = false;
            root.father.children.remove(root);
            for(BGP_Node ch: root.children){
                if(ch.live == true){
                    ch.setFather(root.father);
                    root.father.addChildren(ch);
                }
            }
        }
    }

    public static void main(String[] args) {
        RuleOptimization ro=new RuleOptimization();
        String queryFile="/home/jiguangxi/Desktop/data/ComplexQuery/optionalQuery/filterQuery/filterQuery2";
        ro.ruleOptimizationQuery(queryFile);
    }

}