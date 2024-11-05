package MyQuery.filterTranslator;

import org.apache.jena.sparql.expr.*;

//  遍历表达式树
public class ExprWalker extends ExprVisitorFunction {

    private final ExprVisitor visitor;
    public ExprWalker(ExprVisitor visitor) {
        this.visitor = visitor;
    }

    //  对Function函数进行访问
    @Override
    public void visitExprFunction(ExprFunction func) {
        //  获得当前Function中的每个元素(关系/变量ExprVar/常量NodeValueString)
        for(int i = 1 ; i <= func.numArgs() ; i++ ) {
            Expr expr = func.getArg(i) ;
            if ( expr == null )
                break ;
            expr.visit(this) ;   //  对关系/变量/常量进行访问
        }
        func.visit(visitor) ;
    }

    @Override
    public void visit(ExprFunctionOp funcOp) { funcOp.visit(visitor) ; }

    @Override
    public void visit(NodeValue nv) {
        nv.visit(visitor) ;
//        if(nv.isIRI()){
//            NodeValue.makeInteger(RunDriver.entMap().get(nv.toString())).visit(visitor);
//        }else{
//            nv.visit(visitor) ;
//        }
    }

    @Override
    public void visit(ExprVar nv) { nv.visit(visitor) ; }

    @Override
    public void visit(ExprAggregator eAgg) { eAgg.visit(visitor) ; }

    @Override
    public void visit(ExprNone exprNone) { }
}
