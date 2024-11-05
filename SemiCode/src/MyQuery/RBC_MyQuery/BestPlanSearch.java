package MyQuery.RBC_MyQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by jiguangxi on 2021/4/20 下午6:56
 */
//  todo: 给定一堆孩子结果表的相关信息,寻找最优连接计划
public class BestPlanSearch {

    double costMin=Double.MAX_VALUE;
    int[] posMin;
    int searchTime=0;
    int jianzhi=0;

    public boolean check(double[] a, double[] b){
        if((a[0]<=b[0])&&(a[1]<=b[1])&&(a[0]*b[1]>=a[1]*b[0]))
            return true;
        return  false;
    }

    public void BestSearch(HashSet<Integer> allowedSet,ArrayList<ArrayList<Integer>>  childList,int[] indegree,
                           double[][] tabs,int[] pos,int t,double cost,double curr,double a,double b){
        if(cost>costMin){
            jianzhi++;
            return;
        }
        if(allowedSet.size()==0){
            searchTime++;
            if(cost<costMin){
                costMin=cost;
                posMin=pos.clone();
            }
            return;
        }
        ArrayList<Integer> tmp=new ArrayList<>(allowedSet);
        for(int i=0;i<tmp.size();i++){
            int ppp=tmp.get(i);
            allowedSet.remove(ppp);
            double costi=cost+a*curr*tabs[ppp][0]+b*curr*tabs[ppp][1];
            for(int j=0;j<childList.get(ppp).size();j++){
                int qqq=childList.get(ppp).get(j);
                indegree[qqq]--;
                if(indegree[qqq]==0)
                    allowedSet.add(qqq);
            }
            pos[t]=(int)tabs[ppp][2];
            BestSearch(allowedSet,childList,indegree,tabs,pos,t+1,costi,curr*tabs[ppp][1],a,b);

            for(int j=0;j<childList.get(ppp).size();j++){
                int qqq=childList.get(ppp).get(j);
                indegree[qqq]++;
                if(indegree[qqq]==1)
                    allowedSet.remove(qqq);
            }
            allowedSet.add(ppp);
        }
    }

    // TODO: 启发式方法估计最优代价
    public  int[] findPlan(double[][] tabs){
        costMin=Integer.MAX_VALUE;    // 当前最小代价
        searchTime=0;
        jianzhi=0;
        // todo: 初始化可能用到的数据结构
        HashSet<Integer> allowedSet=new HashSet<>();
        ArrayList<ArrayList<Integer>>  childList=new ArrayList<>();
        int[] indegree=new int[tabs.length];
        int[] pos=new int[tabs.length];
        for(int i=0;i<tabs.length;i++)
            childList.add(new ArrayList<>());
        // todo: 按照R值为第一个关键字降序, EV值为第二关键值升序排序       将大表尽可能放在前面(在驱动表还没怎么扩展的时候进行处理)
        //  按照EV值进行排序  尽可能推迟驱动表的扩展
        // !!!注意 这里经过排序了!!!
        Arrays.sort(tabs,(aa,bb)->{
            if(aa[0]!=bb[0])
                return (int)(bb[0]-aa[0]);
            else
                return (int)(aa[1]-bb[1]);
        });
        //  todo: 计算所有可能的顺序关系
        for(int i=0;i<tabs.length;i++){
            for(int j=i+1;j<tabs.length;j++){
                if(check(tabs[i],tabs[j])==true){
                    childList.get(i).add(j);
                    indegree[j]++;
                }
            }
        }
        //  todo: 第一层的所有可选择节点
        for(int i=0;i<tabs.length;i++){
            if(indegree[i]==0)
                allowedSet.add(i);
        }
        BestSearch(allowedSet,childList,indegree,tabs,pos,0,0,1,0.75,0.25);
        return posMin;
    }
}
