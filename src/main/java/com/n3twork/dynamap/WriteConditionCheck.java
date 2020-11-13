package com.n3twork.dynamap;

import com.n3twork.dynamap.DynamapRecordBean;
import com.n3twork.dynamap.DynamoExpressionBuilder;

public class WriteConditionCheck<T extends DynamapRecordBean> {
    private final Class<T> beanClass;
    private final DynamoExpressionBuilder dynamoExpressionBuilder;
    private final String hashKey;
    private final String rangeKey;

    public WriteConditionCheck(Class<T> beanClass, String hashKey, String rangeKey) {
        this.beanClass = beanClass;
        this.dynamoExpressionBuilder = new DynamoExpressionBuilder(0);
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
    }

    public Class<T> getBeanClass() {
        return beanClass;
    }

    public DynamoExpressionBuilder getDynamoExpressionBuilder() {
        return dynamoExpressionBuilder;
    }

    public String getHashKey() {
        return hashKey;
    }

    public String getRangeKey() {
        return rangeKey;
    }
}
