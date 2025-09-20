package com.fuzzy.common.ast;


import com.fuzzy.common.ast.BinaryOperatorNode.Operator;

public abstract class UnaryOperatorNode<T, O extends Operator> extends UnaryNode<T> {

    protected final O op;

    protected UnaryOperatorNode(T expr, O op) {
        super(expr);
        this.op = op;
    }

    public O getOp() {
        return op;
    }

    @Override
    public String getOperatorRepresentation() {
        return op.getTextRepresentation();
    }

}
