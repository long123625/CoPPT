package MyQuery.nodeClass;

import MyQuery.Utils;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by jiguangxi on 2020/11/25 下午3:04
 */
// TODO: WDPT树的BGP节点  重构
public class BGP_Node {
//    public boolean change=false;    // 　=false已经改变或者不能改变

    // 构建WDPT_SL树,  树上信息
    public int cnt;                             // BGP_Node节点的编号,标识而已
    public int height;                          // WDPT树中当前节点的高度
    public BGP_Node father;                            // 父亲节点
    public ArrayList<BGP_Node> children=new ArrayList<>();  // 孩子节点
    public boolean live=true;                   // 节点状态标记   =false 则后续不再考虑  可以删除
    public boolean isLeaf=false;
    public boolean isSemi = false;
    public String pairFirst;
    public String pairSecond;


    // well-designed查询验证
    public boolean wd=true;                     // 当前节点为根的子树是否为WDPT树
    public int maxIW=0;                         // 当前节点的interface width
    public Set<String> vs=new HashSet<>();                  // 必选变量集
    public Set<String> ws=new HashSet<>();                  // 可选变量集

    public ArrayList<Triple> initTPs;
    public ArrayList<OpFilter>   initOpFilter;

    public HashSet<String> necessaryVarSet;     // 必选变量集
    public HashSet<String> optionVarSet;        // 可选变量集
    public HashSet<HashSet<String>> optionVarGroup;     // 可选变量分组

    // todo: 一个BGP_Node包含的信息:  必选变量集, 可选变量集,  所有Super Node

    public ArrayList<Super_Node> superNodes;

    public ArrayList<Super_Node> superNodesUp;
//    public Set<Super_Node> superNodesUp;

    public Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
    public ArrayList<Expr> allExprs=new ArrayList<>();           // 当前BGP节点所涉及的所有expr条件
    public HashMap<Expr,HashSet<String>> epVars=new HashMap<>();  // 映射: expr条件-->所包含所有变量
    public ArrayList<Expr>  gbExprs=new ArrayList<>();            // 节点级别的Expr条件

    // filterVarGroup:  针对合并的local node的 filter条件  以及全局filter条件
    // filterVarGroupMapping: filter条件对应的分组可空可选变量集
    public HashSet<Expr> filterVarGroup=new HashSet<>();
    public HashMap<Expr,HashSet<String>> filterVarGroupMapping=new HashMap<>();

    //key: 与孩子节点们共享变量集对应的字符串  value:对应了该共享变量集的所有孩子节点
    // 首先按照共享变量数量由小到大排序  数量相同时根据字典序排序
    public TreeMap<String,ArrayList<BGP_Node>> comVar2Ch=new TreeMap<>((a,b)->{
        String[] aa=a.split(",");
        String[] bb=b.split(",");
        if(aa.length!=bb.length)
            return aa.length-bb.length;
        else
            return a.compareTo(b);
    });

    // todo: 初始化一个BGP Node节点
    //输入: BGP模式, 高度, 数组索引,
    public BGP_Node(OpBGP opBgp, int height, int cnt,ArrayList<OpFilter> opFilters){
        this.height=height;
        this.cnt=cnt;
        this.initTPs=new ArrayList<>(opBgp.getPattern().getList());     // 会变的
        this.initOpFilter=opFilters;
    }

    // 获得的是initTPs中的变量  在SL之前使用 :  规则4 5优化,  判断是否为Well desinged查询
    public HashSet<String> getInitVars(){
        return Utils.getVarsOfTps(this.initTPs,"_");
    }


    // 变量集,  主语变量集,  (subject, tab list),  (主语, (数据表, 三元组模式集)),  (主语, (数据表, 布尔值))
    public void initBGPSL(HashSet<String> allVars,HashSet<String> subSets,HashMap<String, HashSet<String>> varsTable,
                          HashMap<String,HashMap<String,ArrayList<Triple>>> ln2Triples,HashMap<String, HashMap<String,Boolean>> ln2Avi){
        this.necessaryVarSet=new HashSet<>(allVars);    //初始化当前BGP的必选变量集
        this.optionVarSet=new HashSet<>();
        this.optionVarGroup=new HashSet<>();

        // 构造对应的所有Super Node
        this.superNodes=new ArrayList<>();
        this.superNodesUp = new ArrayList<>();
//        this.superNodesUp = new HashSet<>();
        for(String sub:ln2Triples.keySet()){
            Super_Node sn = new Super_Node(sub, ln2Triples.get(sub));
            this.superNodes.add(sn);
        }
    }



    // todo: 判断当前节点中fiter是否都为safe filter
    public boolean checkFilter(){
        HashSet<String> initVars=new HashSet<>(getInitVars());
        for(OpFilter of:this.initOpFilter){
            for(Expr ep:of.getExprs()){
                if(!initVars.containsAll(getVarsExpr(ep)))
                    return false;
            }
        }
        return true;
    }
    // todo: 给定一个Expr条件, 获得其涉及的变量集合
    public HashSet<String> getVarsExpr(Expr ep){
        HashSet<String> vv=new HashSet<>();
        for(Var v:ep.getVarsMentioned())
            vv.add(v.toString().replace("?","_"));
        return vv;
    }

    // todo: 组合型Filter拆分  所有Expr条件
    public void splitFilter(){
        for(OpFilter of:this.initOpFilter){
            for(Expr ep:of.getExprs()){
                splitExpr(ep);      // &&型组合查询, 拆分出所有子filter 也加入进来
            }
        }
    }
    public void splitExpr(Expr ep){
        if(ep.getFunction().getFunctionSymbol().equals("symbol:bound")) // bound型直接跳过
            return;
        if(ep.getFunction().getFunctionSymbol().equals("symbol:not")){  // !bound则直接当前节点结果不再需要
            live=false;
            return;
        }
        HashSet<String> vars=getVarsExpr(ep);
        epVars.put(ep,vars);
        allExprs.add(ep);
        if(!ep.getFunction().getFunctionSymbol().toString().equals("symbol:and"))
            return ;
        for(Expr e:ep.getFunction().getArgs())          // 对于&&组合型进行拆分
            splitExpr(e);
    }


    // todo: 对Exprs进行排序   目前考虑的: (1) 等号放前面,其他放后面 不等号最后;  (2) 常量放前面,变量放后面;  (3) 原子放前面 组合放后面
    public void sortExpr(){
        HashMap<String,Integer> opScore=new HashMap<>();        // 定义操作符优先级
        String[] ops=new String[]{"=",">","<",">=","<=","!=","&&","||"};
        for(int i=0;i<ops.length;i++)
            opScore.put(ops[i],i);
        // 按照约束大小进行排序
        allExprs.sort((a,b)->{
            // map 对opName排序,   map值相同时再看变量的数量
            if(opScore.get(a.getFunction().getOpName())!=opScore.get(b.getFunction().getOpName())){
                return opScore.get(a.getFunction().getOpName())-opScore.get(b.getFunction().getOpName());
            }else{
                return a.getFunction().getVarsMentioned().size()-b.getFunction().getVarsMentioned().size();
            }
        });
        System.out.println(allExprs);
    }

    // todo: 对于每个local node, 遍历allExprs中所有条件, 满足变量集包含关系的话则联系到一起
    public void exprToLN(){
        HashSet<Expr> vis=new HashSet<>();
        for(Super_Node sn:this.superNodes){         //  Super节点
            for(Local_Node ln:sn.localnodes){       //  Local节点
                HashSet<String> lnVars=ln.getVars();
                for(Expr ep:allExprs){
                    if(lnVars.containsAll(epVars.get(ep))){
                        ln.exprs.add(ep);
                        vis.add(ep);
                    }
                }
            }
        }
        for(Expr ep:allExprs){
            if(!vis.contains(ep))
                gbExprs.add(ep);
        }
    }

    public int getLiveChildren(){
        int liveChildren=0;
        for(BGP_Node ch:this.children){
            if(ch.live==true)
                liveChildren++;
        }
        return liveChildren;
    }

    // todo: 依据公共变量 对孩子节点进行分块, 并且为每个孩子构造  与父亲公共变量
    public void getCh2ComVar(){
        for(BGP_Node ch:this.children) {
            // 获得所有公共变量,  排序   连接到一起
            HashSet<String> shareVarsFather=new HashSet<>(ch.necessaryVarSet);
            shareVarsFather.retainAll(this.necessaryVarSet);
            ArrayList<String> al=new ArrayList<>(shareVarsFather);
            Collections.sort(al);
            String ss=String.join(",", al);
            if(!this.comVar2Ch.containsKey(ss))
                this.comVar2Ch.put(ss,new ArrayList<>());
            this.comVar2Ch.get(ss).add(ch); //共享变量 : 孩子节点列表,即相同共享变量的孩子节点列表
        }
    }
    // todo: 获得某变量的最小可能取值数
//    public int getMaxValue(String var, HashMap<String, Integer> preSval, HashMap<String, Integer> preOval){
//        int maxvalue=0;
//        int maxx=Integer.MAX_VALUE;
//        for(Triple tp:this.getBGPTPs()){
//            String pre=pattern.matcher(tp.getPredicate().toString()).replaceAll("").toLowerCase();
//            if(tp.getSubject().toString().replace("?","_").equals(var)) // 作为pre的主语
//                maxx=Math.min(maxx,preSval.getOrDefault(pre,10000));
//            if(tp.getObject().toString().replace("?","_").equals(var)&&preOval.containsKey(var))
//                maxx=Math.min(maxx,preOval.getOrDefault(pre,10000));
//        }
//        return maxvalue;
//    }
    public List<String> getMaxValue(String var, HashMap<String, Integer> preSval, HashMap<String, Integer> preOval){
//        System.out.println("jisuan jishu ");
        int maxvalue = 0;
        int maxx = Integer.MAX_VALUE;
        String preMin = "";
        String subVar = "";
        String objVar = "";
        String varPos = "";
        List<String> res = new ArrayList<>();
        System.out.println(this.getBGPTPs().size());
        for(Triple tp: this.getBGPTPs()){ //遍历一个孩子节点的所有三元组
            String pre = pattern.matcher(tp.getPredicate().toString()).replaceAll("").toLowerCase();
            System.out.println(pre + " " + tp.getObject() + " " + tp.getSubject() + " " + maxx + " " + var);
            //把对应的谓词保留记录下来，后续子查询的时候，判断是不是有该谓词

            if(tp.getSubject().toString().replace("?","_").equals(var)) // 作为pre的主语
            {
                System.out.println("var is subject pos iter");
                if( maxx > preSval.getOrDefault(pre, 10000)) {
                    maxx = preSval.getOrDefault(pre, 10000);
                    preMin = pre;
                    varPos = "subject";
                }
            }
            //                maxx = Math.min(maxx, preSval.getOrDefault(pre,10000));
            //这行代码就有问题,preOval怎么会有变量,这个是读取文件得到的每个谓词对应的不同宾语数 && preOval.containsKey(var)
            if(tp.getObject().toString().replace("?","_").equals(var) )
            {
                System.out.println( " var is object pos iter");
                if( maxx > preOval.getOrDefault(pre, 10000)) {
                    maxx = preOval.getOrDefault(pre, 10000);
                    preMin = pre;
                    varPos = "object";
                }
            }
//                maxx = Math.min(maxx, preOval.getOrDefault(pre,10000));
        }
        System.out.println(maxx + " " + " gujiyizhis 10000");
        System.out.println("varPos is" + " " + varPos );
        //记录一个孩子节点最后的结果
        res.add(String.valueOf(maxx));
        if(varPos.equals("subject")) {
            System.out.println(" pos is subject");
            //说明最终是主语位置的变量，添加三元组
            res.add(var); // 主语变量
            res.add(preMin); // 谓词
            res.add("0");
        }
        else {
            System.out.println(" pos is object");
            //说明最终是宾语位置的变量，添加三元组
            res.add(var);
            res.add(preMin);

            res.add("2");
        }
        System.out.println(res);
        return  res;
//        return maxvalue;
    }



    public void setFather(BGP_Node fa){
        this.father=fa;
    }

    public void addChildren(BGP_Node chi){
        this.children.add(chi);
    }


    // 动态获得当前BGP目前的所有三元组模式
    public ArrayList<Triple> getBGPTPs(){
        ArrayList<Triple> tps=new ArrayList<>();
        for(Super_Node sn:this.superNodes){
            for(Local_Node ln:sn.localnodes){
                tps.addAll(ln.getTPs());
            }
        }
        return tps;
    }
    // 动态获得当前BGP目前的所有变量
    public HashSet<String> getBGPVars(){
        HashSet<String> vars=new HashSet<>();
        for(Super_Node sn:this.superNodes){
            for(Local_Node ln:sn.localnodes){
                vars.addAll(ln.getVars());
            }
        }
        return vars;
    }
    // 判断是否存在指定模式的Local Node
    public Local_Node findLocalNodeBySchema(String sub, String tab){
        for(Super_Node sn:this.superNodes){
            for(Local_Node ln:sn.localnodes){
                if(ln.sameSchema(sub,tab))
                    return ln;
            }
        }
        return null;
    }

    //从移动下来的 supernode中判断是否有相同schema 的local node
    public Local_Node findLocaNodeUpBySchema(String sub, String tab) {
        if(this.superNodesUp.size() == 0) return null;
        for(Super_Node sn : this.superNodesUp) {
            for(Local_Node ln : sn.localnodes) {
                if(ln.sameSchema(sub, tab)) {
                    return ln;
                }
            }
        }
        return null;
    }

    //判断是否有相同主语的super node
    public Super_Node findSuperNodeBySubject(String sub){
        for(Super_Node sn:this.superNodes){
            if(sn.subject.equals(sub)) {
                return sn;
            }
        }
        return null;
    }

    //从合并下来的superNode中判断是否有相同的supernode
    public  Super_Node findSuperNodeUpBySubject(String sub) {
        if(this.superNodesUp.size() == 0) return null;
        for (Super_Node sn : this.superNodesUp) {
            if(sn.subject.equals(sub)) {
                return sn;
            }
        }
        return null;
    }



    // 判断是否只有一个SuperNode
    public Boolean isSingleSuperNode(){
        return superNodes.size()==1;
    }
    // 获得唯一Super Node
    public Super_Node getSingleSuperNode(){
        return superNodes.get(0);
    }

    //我添加:获得所有的Super Node
    public ArrayList<Super_Node> getAllSuperNode(){
        return superNodes;
    }

    // 判断是否只有一个Local Node
    public Boolean isSingleLocalNode(){
        return superNodes.size()==1&&superNodes.get(0).localnodes.size()==1;
    }
    // 获得唯一Local Node
    public Local_Node getSingleLocalNode(){
        return superNodes.get(0).localnodes.get(0);
    }

    // 移除无效节点
    public void deleteUnVaildNode(Local_Node deleteLN){
        ArrayList<Super_Node> snBack=new ArrayList<>(superNodes);
        for(Super_Node sn:snBack){
            ArrayList<Local_Node> lnBack=new ArrayList<>(sn.localnodes);
            for(Local_Node ln:lnBack){
                if(ln.sameSchema(deleteLN.subject,deleteLN.table))
                    sn.localnodes.remove(ln);
            }
            if(sn.localnodes.size()==0)
                superNodes.remove(sn);
        }
        if(superNodes.size()==0)
            this.live=false;
    }

    public HashSet<String> getAllVars(String rep){
        HashSet<String> allVars=new HashSet<>();
        allVars.addAll(necessaryVarSet);
        allVars.addAll(optionVarSet);
        return allVars;
    }


//    public HashSet<String> getAllVars(String rep){
//        HashSet<String> allVars=new HashSet<>();
//        for(Triple tp:this.getBasicPattern()){
//            String sub = tp.getSubject().toString().replace("?",rep);
//            String obj = tp.getObject().toString().replace("?",rep);
//            String pre = pattern.matcher(tp.getPredicate().toString()).replaceAll("").toLowerCase();
//            if (sub.startsWith(rep))
//                allVars.add(sub);
//            if (obj.startsWith(rep))
//                allVars.add(obj);
//        }
//        return allVars;
//    }
//
//    public BasicPattern getBasicPattern(){
//        BasicPattern bp=new BasicPattern();
//        for(String sub : ln2Avi.keySet()){
//            for(String ln: ln2Avi.get(sub).keySet()){
//                if(ln2Avi.get(sub).get(ln)==true){
//                    for(Triple tp:ln2Triples.get(sub).get(ln))
//                        bp.add(tp);
//                }
//            }
//        }
//        this.basicPattern=bp;
//        return this.basicPattern;
//    }

    public HashSet<String> getShareVarsFather(){
        if(father==null) return  null;
        HashSet<String> shareVars=new HashSet<>(getBGPVars());
        shareVars.retainAll(father.getBGPVars());
        return  shareVars;
    }

    public String toString(){
        String res="BGP  height:"+height+"  cnt:"+cnt+" state:"+live;
        if(father!=null)
            res+=" father:"+father.cnt+" chidlren:[";
        else
            res+=" father:-1 chidlren:[";
        for(BGP_Node x : children){
            res+=x.cnt+" ";
        }
        res+="]  optionalVarSet: "+optionVarSet+ " optionalVarGroup: "+optionVarGroup+"  necessaryVarSet: "+necessaryVarSet;
        res+=" shareVarsFather: "+getShareVarsFather();
        res+=" basicPattern \n "+getBGPTPs().toString();
        return res;
    }

}
