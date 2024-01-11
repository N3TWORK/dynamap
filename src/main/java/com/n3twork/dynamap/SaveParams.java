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

package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaveParams<T extends DynamapRecordBean> {

    private T dynamapRecordBean;

    private boolean disableOverwrite;
    private boolean disableOptimisticLocking;
    private DynamoRateLimiter writeLimiter;
    private String suffix;
    private Map<String, Object> values;
    private Map<String, String> names;
    private List<String> conditionExpressions = new ArrayList<>();
    private ReturnValuesOnConditionCheckFailure returnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.NONE;

    private SaveParams() {
    }

    public SaveParams(T dynamapRecordBean) {
        this.dynamapRecordBean = dynamapRecordBean;
    }

    public SaveParams<T> withDisableOverwrite(boolean disableOverwrite) {
        this.disableOverwrite = disableOverwrite;
        return this;
    }

    public SaveParams<T> withDisableOptimisticLocking(boolean disableOptimisticLocking) {
        this.disableOptimisticLocking = disableOptimisticLocking;
        return this;
    }

    public SaveParams<T> withWriteLimiter(DynamoRateLimiter writeLimiter) {
        this.writeLimiter = writeLimiter;
        return this;
    }

    public SaveParams<T> withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }

    public SaveParams<T> withConditionExpressions(List<String> conditionExpressions) {
        this.conditionExpressions = conditionExpressions;
        return this;
    }

    public SaveParams<T> withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure returnValuesOnConditionCheckFailure) {
        this.returnValuesOnConditionCheckFailure = returnValuesOnConditionCheckFailure;
        return this;
    }

    public SaveParams<T> withNames(Map<String, String> names) {
        this.names = names;
        return this;
    }

    public SaveParams<T> withValues(Map<String, Object> values) {
        this.values = values;
        return this;
    }

    ////////


    public T getDynamapRecordBean() {
        return dynamapRecordBean;
    }

    public boolean isDisableOverwrite() {
        return disableOverwrite;
    }

    public boolean isDisableOptimisticLocking() {
        return disableOptimisticLocking;
    }

    public DynamoRateLimiter getWriteLimiter() {
        return writeLimiter;
    }

    public String getSuffix() {
        return suffix;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Map<String, String> getNames() {
        return names;
    }

    public List<String> getConditionExpressions() {
        return conditionExpressions;
    }

    public ReturnValuesOnConditionCheckFailure getReturnValuesOnConditionCheckFailure() { return returnValuesOnConditionCheckFailure; }
}
