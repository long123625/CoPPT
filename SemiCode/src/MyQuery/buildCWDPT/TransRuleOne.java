package MyQuery.buildCWDPT;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Created by jiguangxi on 2021/11/3 下午10:47
 */
public class TransRuleOne extends TransformBase {

    @Override
    public Op transform(OpBGP opbGP){
        RuleOptimization.st.push(opbGP);
        return opbGP;
    }

    //  todo: 对filter转换:  filter后面紧跟leftjoin的情况
    @Override
    public Op transform(OpFilter opfilter, Op subOp) {
        Op ssubOp = RuleOptimization.st.pop();
        if(ssubOp instanceof OpLeftJoin){
            // 这里需要再注意一下  opfilter的所有变量应该都属于ssubOp
            // 获得opfilter的变量集, 获得ssubOp的变量集,然后判断
            //            Set<Var> filterVars=opfilter.getExprs().getVarsMentioned();     //  获得opfilter的变量集
            OpLeftJoin opleftjoin = (OpLeftJoin)ssubOp;
            OpFilter opFilterNew = OpFilter.filterAlways(opfilter.getExprs(), opleftjoin.getLeft());
            OpLeftJoin opLeftJoinNew = OpLeftJoin.createLeftJoin(opFilterNew, opleftjoin.getRight(), opleftjoin.getExprs());
            RuleOptimization.st.push(opLeftJoinNew);
            return opfilter;
        }else{
            RuleOptimization.st.push(opfilter);
            return opfilter;
        }
    }

    // todo: 对join节点进行优化   主要分为9种情况??
    @Override
    public Op transform(OpJoin join, Op left, Op right) {// 下面的转变并没有更改join的.
        Op rright=RuleOptimization.st.pop();
        Op lleft=RuleOptimization.st.pop();
        if(lleft instanceof OpLeftJoin && rright instanceof OpBGP){
            OpLeftJoin opleftjoin=(OpLeftJoin)lleft;
            OpBGP opbgpNec=(OpBGP)getNecessaryOp(opleftjoin.getLeft());   //左孩子必要BGP
            opbgpNec.getPattern().addAll(((OpBGP)rright).getPattern());   // 将右孩子加入到opbgpNec中即可
            RuleOptimization.st.push(lleft);;
        }else if(rright instanceof OpLeftJoin  && lleft instanceof OpBGP){
            OpLeftJoin opleftjoin=(OpLeftJoin)rright;
            OpBGP opbgpNec=(OpBGP)getNecessaryOp(opleftjoin.getLeft());   //左孩子必要BGP
            opbgpNec.getPattern().addAll(((OpBGP)lleft).getPattern());   // 将右孩子加入到opbgpNec中即可
            RuleOptimization.st.push(rright);
        }else if(lleft instanceof  OpLeftJoin && rright instanceof OpLeftJoin){
            OpLeftJoin leftSon=(OpLeftJoin)lleft;
            OpLeftJoin last=(OpLeftJoin)lleft;

            OpLeftJoin rightSon=(OpLeftJoin)rright;
            OpBGP leftNeceBgp=(OpBGP)getNecessaryOp(leftSon);
            OpBGP rightNeceBgp=(OpBGP)getNecessaryOp(rightSon);
            leftNeceBgp.getPattern().addAll(rightNeceBgp.getPattern()); // 加入到右边去

            while(true){
                OpLeftJoin opleftnew=OpLeftJoin.createLeftJoin(last,rightSon.getRight(),null);
                last=opleftnew;
                if(rightSon.getLeft() instanceof  OpBGP)
                    break;
                rightSon=(OpLeftJoin)rightSon.getLeft();
            }
            RuleOptimization.st.push(last);

        } else if(lleft instanceof OpBGP && rright instanceof OpBGP) {      // 左右都是BGP直接合并为一个
            ((OpBGP) lleft).getPattern().addAll(((OpBGP) rright).getPattern());
            RuleOptimization.st.push(lleft);
        }else if(lleft instanceof OpLeftJoin && rright instanceof OpFilter){
            OpLeftJoin leftSon=(OpLeftJoin)lleft;
            OpFilter  rightSon=(OpFilter)rright;
            OpBGP     rightSSon=(OpBGP)rightSon.getSubOp();
            OpBGP leftNeceBgp=(OpBGP)getNecessaryOp(leftSon);
            Op newop=leftFilterTransform(leftSon,rightSon,leftNeceBgp);
            leftNeceBgp.getPattern().addAll(rightSSon.getPattern());
            RuleOptimization.st.push(newop);
        }else if(lleft instanceof OpFilter && rright instanceof OpLeftJoin){
            OpLeftJoin rightSon=(OpLeftJoin)rright;
            OpFilter  leftSon=(OpFilter)lleft;
            OpBGP     leftSSon=(OpBGP)leftSon.getSubOp();
            OpBGP leftNeceBgp=(OpBGP)getNecessaryOp(rightSon);
            Op newop=leftFilterTransform(rightSon,leftSon,leftNeceBgp);
            leftNeceBgp.getPattern().addAll(leftSSon.getPattern());
            RuleOptimization.st.push(newop);
        }else if(lleft instanceof  OpFilter && rright instanceof OpFilter){
            OpFilter leftSon=((OpFilter)lleft);
            OpFilter rightSon=((OpFilter)rright);
            OpBGP leftSSon=((OpBGP)leftSon.getSubOp());
            OpBGP rightSSon=((OpBGP)rightSon.getSubOp());
            leftSon.getExprs().addAll(rightSon.getExprs());
            leftSSon.getPattern().addAll(rightSSon.getPattern());
            RuleOptimization.st.push(leftSon);
        }else if(lleft instanceof  OpFilter && rright instanceof OpBGP){
            OpFilter leftSon=((OpFilter)lleft);
            OpBGP rightSon=((OpBGP)rright);
            OpBGP leftSSon=((OpBGP)leftSon.getSubOp());
            leftSSon.getPattern().addAll(rightSon.getPattern());
            RuleOptimization.st.push(leftSon);
        }else if(lleft instanceof  OpBGP && rright instanceof OpFilter){
            OpBGP leftSon=((OpBGP)lleft);
            OpFilter rightSon=((OpFilter)rright);
            OpBGP rightSSon=((OpBGP)rightSon.getSubOp());
            rightSSon.getPattern().addAll(leftSon.getPattern());
            RuleOptimization.st.push(rightSon);
        }else{
            RuleOptimization.st.push(join);
        }
        return join;
    }

    // todo: left join: 没什么转换的, 先将左右孩子出栈(左右孩子可以为任何模式),新建节点然后加入即可
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        Op rright=RuleOptimization.st.pop();
        Op lleft=RuleOptimization.st.pop();
        OpLeftJoin opLeftJoinNew=OpLeftJoin.createLeftJoin(lleft,rright,opLeftJoin.getExprs());
        RuleOptimization.st.push(opLeftJoinNew);
        return opLeftJoin;
    }

    // todo: join的两个孩子分别为LeftJoin与Filter的情况
    public Op leftFilterTransform(Op leftSon, Op rightSon,Op leftNeceBgp){
        if(leftSon instanceof OpBGP)
            return leftSon;
        if(leftSon instanceof OpLeftJoin && ((OpLeftJoin) leftSon).getLeft().toString().equals(leftNeceBgp.toString())){
            OpFilter opfilter=OpFilter.filterAlways(((OpFilter)rightSon).getExprs(),leftNeceBgp);
            return OpLeftJoin.createLeftJoin(opfilter,((OpLeftJoin) leftSon).getRight(),null);
        }
        if(leftSon instanceof OpFilter){
            if(((OpFilter)leftSon).getSubOp().toString().equals(leftNeceBgp.toString()))
                ((OpFilter) leftSon).getExprs().addAll(((OpFilter) rightSon).getExprs());
            Op opp=leftFilterTransform(((OpFilter)leftSon).getSubOp(),rightSon,leftNeceBgp);
            return OpFilter.filterAlways(((OpFilter)leftSon).getExprs(),opp);
        }
        Op opp1=leftFilterTransform(((OpLeftJoin)leftSon).getLeft(),rightSon,leftNeceBgp);
        Op opp2=leftFilterTransform(((OpLeftJoin)leftSon).getRight(),rightSon,leftNeceBgp);
        return OpLeftJoin.createLeftJoin(opp1,opp2,null);
    }

    // todo: 给定一个op,找到其中必要BGP
    public Op getNecessaryOp(Op op){
        if(op instanceof OpBGP)
            return op;
        else if(op instanceof Op1)      // filter情况  在下面去找
            return getNecessaryOp(((Op1) op).getSubOp());
        else
            return getNecessaryOp(((Op2)op).getLeft()); // left join情况 在左边去找
    }
}