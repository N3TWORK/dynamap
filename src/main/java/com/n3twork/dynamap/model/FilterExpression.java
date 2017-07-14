package com.n3twork.dynamap.model;

import java.util.HashMap;
import java.util.Map;

public class FilterExpression {

    private String expressionString;
    private Map<String, Object> values = new HashMap<>();
    private Map<String, String> names = new HashMap<>();


    public FilterExpression withFilterExpression(String expression) {
        this.expressionString = expression;
        return this;
    }

    public FilterExpression withNames(Map<String, String> names) {
        this.names = names;
        return this;
    }

    public FilterExpression withValues(Map<String, Object> values) {
        this.values = values;
        return this;
    }

    public String getExpressionString() {
        return expressionString;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Map<String, String> getNames() {
        return names;
    }
}
