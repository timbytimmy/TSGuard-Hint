package com.fuzzy.griddb.ast;

public class GridDBText implements GridDBExpression {

    private final String text;

    public GridDBText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
