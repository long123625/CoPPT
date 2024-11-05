package MyQuery.buildCWDPT;

import MyQuery.nodeClass.BGP_Node;
import org.apache.jena.ext.com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by jiguangxi on 2021/3/18 下午2:44
 * todo:  判断是否符合well-designed情况
 */
public class WDTest {
    //  todo:每个WDPT节点都维护三部分信息: vs必选变量集, ws可选变量集 , maxIW节点interface width
    public static void isPattern(BGP_Node root){
        if(root.children.size()==0)     // 叶子节点满足well-designed情况
            return ;
        HashSet<String> chvars=new HashSet<String>();
        for(int i=0;i<root.children.size();i++){
            BGP_Node ch=root.children.get(i);
            chvars.addAll(ch.getInitVars());
            isPattern(ch);              // 递归判断孩子节点
            boolean test= Sets.union(
                    // 父节点的可选变量集  与 子节点的所有变量的交集
                    // (若交集不为空,则说明当前孩子有,另一个孩子有,但是父亲没有 不连通了)
                    Sets.intersection(root.ws,Sets.union(ch.vs,ch.ws)),
                    // 子节点的可选变量集  与 父节点的所有变量的交集
                    // (子节点的孩子们有, 父节点有, 但是子节点没有   不连通了)
                    Sets.intersection(ch.ws,Sets.union(root.vs,root.ws))
            ).isEmpty();
            if(test){   // 如果交集都为空,则可以进一步判断  并且设置更新当前父节点的可选变量集
                root.wd=root.wd&&ch.wd;
                // 结合当前孩子节点变量集,更新根节点的可选变量集
                // 这里ws是会进行更新的
                root.ws= Sets.union(Sets.difference(ch.vs, root.vs), Sets.union(root.ws, ch.ws));
            }else{
                root.wd=false;
            }
        }
        chvars.retainAll(root.getInitVars()); //效果和取交集一样
        root.maxIW=chvars.size(); //宽度就是master节点和slave节点的公共变量的个数,所以不止是1个
    }

    // todo: 判断一个查询是否为well-designed查询
    public static  boolean isUwd(ArrayList<BGP_Node> bgp_nodes){
        isPattern(bgp_nodes.get(0));
        return bgp_nodes.get(0).wd;
    }

    // todo: 判断是否所有节点的interface width<=1
    public static int maxInterfaceWidth(ArrayList<BGP_Node> bgp_nodes){
        int maxIW=0;
        for(BGP_Node bgp:bgp_nodes){
            if(bgp.children.size()>0){
                HashSet<String> chvars=new HashSet<String>();
                // 该节点的所有孩子节点的变量集的并集
                for(BGP_Node ch:bgp.children){
                    if(ch.live==true)
                        chvars.addAll(ch.getInitVars());
                }
                chvars.retainAll(bgp.getInitVars());// 判断交集元素个数
                maxIW=Math.max(maxIW , chvars.size());
            }
        }
        return maxIW;
    }

    public static boolean filterCheck(ArrayList<BGP_Node> bgp_nodes){
        for(BGP_Node bgp:bgp_nodes)
            if(bgp.checkFilter()==false)
                return false;
        return true;
    }
    // todo: 判断是否为CQof查询
    public static boolean isCQof(ArrayList<BGP_Node> bgp_nodes){
        boolean wd=isUwd(bgp_nodes);        //  是否为well-designed查询
        int maxIW=0;                        // interface width是否小于等于1
        for(BGP_Node bgp:bgp_nodes){
            if(bgp.live==true)
                maxIW=Math.max(maxIW,bgp.maxIW);
        }

        if(wd==true && maxIW==1 &&filterCheck(bgp_nodes)==true){
            return true;
        } else{
            return false;
        }

    }
}
