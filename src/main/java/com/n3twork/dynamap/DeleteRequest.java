/*
    Copyright 2017 N3TWORK INC

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

package com.n3twork.dynamap;

import java.util.Map;

public class DeleteRequest<T extends DynamapRecordBean> {
    private String hashKeyValue;
    private Object rangeKeyValue;
    private final Class<T> resultClass;
    private String conditionExpression;
    private Map<String, Object> values;
    private Map<String, String> names;

    public DeleteRequest(Class<T> resultClass) {
        this.resultClass = resultClass;
    }


    public DeleteRequest<T> withHashKeyValue(String hashKeyValue) {
        this.hashKeyValue = hashKeyValue;
        return this;
    }

    public DeleteRequest<T> withRangeKeyValue(Object rangeKeyValue) {
        this.rangeKeyValue = rangeKeyValue;
        return this;
    }

    public DeleteRequest<T> withConditionExpression(String conditionExpression) {
        this.conditionExpression = conditionExpression;
        return this;
    }


    public DeleteRequest<T> withNames(Map<String, String> names) {
        this.names = names;
        return this;
    }

    public DeleteRequest<T> withValues(Map<String, Object> values) {
        this.values = values;
        return this;
    }

    public String getHashKeyValue() {
        return hashKeyValue;
    }

    public Object getRangeKeyValue() {
        return rangeKeyValue;
    }

    public Class<T> getResultClass() {
        return resultClass;
    }

    public String getConditionExpression() {
        return conditionExpression;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Map<String, String> getNames() {
        return names;
    }
}
