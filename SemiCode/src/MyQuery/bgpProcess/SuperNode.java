package MyQuery.bgpProcess;

import scala.Long;

import java.util.*;

//   一个超点类
//   为SPARQL查询中的每个主语变量  构建一个超点
public class SuperNode {
    //  该超点涉及到的所有顶点？
    private ArrayList<String> vertexes=new ArrayList<>();
    //
    private ArrayList<DSP> DSPlist=new ArrayList<>(); //data sub-pattern list
    //  该主语
    private String subject;
    //  超点对应的主语  相关的所有三元组模式----以该超点为主语的所有三元组模式
    private ArrayList<String[]> starEdges;
    //  超点中的常量元素个数
    private int SNconstantNumber=0;
    //  超点的基数值
    private double SNcardinality;
    //  超点对应的最小表大小？？？？
    private double minTableSize=Double.MAX_VALUE;

    //  构建超点
    //  输入的是查询中的一个主语及其对应的所有三元组模式
    public SuperNode(Map.Entry<String,ArrayList<String[]>> subjectStar){
        //  设置超点对应的主语、对应的星型边/图，更新顶点列表
        this.subject=subjectStar.getKey();
        this.starEdges=subjectStar.getValue();
        vertexes.add(subject);
    }

    //  获得超点对应的基数值
    public double getSNcardinality(){
        return this.SNcardinality;
    }

    //  获得超点的常量个数
    public int getSNconstantNumber(){return this.SNconstantNumber;}

    //  设置超点对应的基数值
    public void setSNcardinality(double newCardinality){
        this.SNcardinality=newCardinality;
    }

    //  获得超点对应的DSP列表(超点所涉及的局部点列表---该主语涉及到了几个SPT表)
    public ArrayList<DSP> getDSPlist(){
        return this.DSPlist;
    }

    //  获得超点对应的顶点列表(该超点涉及到的所有顶点)
    public  ArrayList<String> getVertexes(){
        return this.vertexes;
    }

    //
    public double getMinTableSize(){return this.minTableSize;}

    //  获得超点对应的主语
    public String getSubject(){return this.subject;}

    //  设置超点对应的DSP列表(数据子模式列表，就是局部点列表)
    // 输入： 谓词到SPT表的索引，  SPT表内容及其大小
    public void setDSPList(scala.collection.Map<String, String[]> pIndexSchema, scala.collection.Map<String, Long> dataPatternTableSize){
        //  超点所涉及的三元组模式个数
        int starEdgesSize=starEdges.size();
        //  遍历所有的三元组模式
        while(starEdgesSize!=0){
            String adjVar;
            // 获得当前列表中的第一个三元组模式
            String[] starEdge=starEdges.get(0);
            //  根据三元组模式的谓词，找到其对应的SPT模式
            String p1 = starEdge[1];
            List<String> schema= Arrays.asList(pIndexSchema.get(p1).get());
            //  继续遍历剩余的三元组模式，将同属于该SPT模式的所有语句及顶点都找出，用于构造一个局部点DSP
            ArrayList<String[]> candidateEdges=new ArrayList<>();
            //  局部点涉及的常量元素的个数
            int constantNumber=subject.startsWith("?")?0:1;
            ArrayList<String> DSPvertexes=new ArrayList<>();
            DSPvertexes.add(subject);
            //  遍历其他三元组模式，谓词同属于一个SPT模式，则放到同一个DSP中
            for(String[] edge:starEdges){
                if(schema.contains(edge[1])){
                    candidateEdges.add(edge);
                    DSPvertexes.add(edge[2]);
                    if(!edge[2].startsWith("?")) constantNumber++;
                }
            }
            //  更新统计信息，
            vertexes.addAll(DSPvertexes);
            starEdges.removeAll(candidateEdges);
            starEdgesSize-=candidateEdges.size();
            //  构建一个DSP局部点   输入 主语，该DSP涉及到的所有边、所有点，该DSP对应的SPT模式，
            DSP dsp=new DSP(subject,candidateEdges,schema,DSPvertexes);
            //  为DSP设置基数值   传递的是该中包含的常量元素个数，对应SPT表大小
            dsp.setCardinality(constantNumber,dataPatternTableSize.get(dsp.getTableName()).get()+"");
            //  统计超点中的总的常量值，这里应该减去主语的常量计数吧？？
            SNconstantNumber+=constantNumber;
            //  更新该超点对应的局部点列表
            DSPlist.add(dsp);
        }
        // 对该超点对应的所有局部点按照基数值从小到大排序
//        // todo: 打印局部点的基数值
//        for(DSP dsp:DSPlist)
//            System.out.println(dsp.getSchema()+"  "+dsp.getCardinality()+"  "+dsp.getTableSize());
        DSPlist.sort(new Comparator<DSP>() {
            @Override
            public int compare(DSP o1, DSP o2) {
                double diff=o1.getCardinality()-o2.getCardinality();
                if(diff<=0) return -1;
                else return 1;
            }
        });
        // 计算超点对应的基数值    局部点中最小基数值 除以超点中的总常量个数
        SNcardinality=SNconstantNumber==0?Double.MAX_VALUE:DSPlist.get(0).getCardinality()/SNconstantNumber;
        // 该超点对应的最小表大小----超点包含的局部点对应的表大小的最小值
        for(DSP dsp:DSPlist){
            minTableSize=Math.min(minTableSize,dsp.getTableSize());
        }
        // 没有常量时，基数值即为最小表大小
        if(SNcardinality==Double.MAX_VALUE) SNcardinality=minTableSize;
    }

    public void print(){
        System.out.println(this.getSubject()+":");
        for(DSP cur_dp : this.getDSPlist()){
            for(SuperNode sp:cur_dp.getNextSuperNodeList()){
//                System.out.println(sp.getSubject()+":");
            }
        }
    }
}