/*
    Copyright 2018 N3TWORK INC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
