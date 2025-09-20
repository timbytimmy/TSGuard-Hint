package com.fuzzy.TDengine.ast;

public class TDengineText implements TDengineExpression {

    private final String text;

    public TDengineText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
