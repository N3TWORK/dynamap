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

import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.util.*;

public class DynamoExpressionBuilder {

    private ObjectMapper objectMapper;

    private static final Set<String> SUPPORTED_JAVA_TYPES = ImmutableSet.of("java.lang.Integer", "java.lang.Long", "java.lang.Float", "java.lang.Double", "java.lang.Number", "java.lang.BigDecimal", "java.lang.String");

    private final List<String> addSection = new ArrayList<>();
    private final List<String> setSection = new ArrayList<>();
    private final List<String> removeSection = new ArrayList<>();
    private final Alias names;
    private final Alias vals;
    private final Alias condVals;
    private final List<String> conditions = new ArrayList<>();

    private NameMap nameMap = new NameMap();
    private ValueMap valueMap = new ValueMap();

    public enum ComparisonOperator {

        EQUALS("="), NOT_EQUALS("!="), LESS_THAN("<"), LESS_THAN_EQUAL_TO("<="), GREATHER_THAN(">"), GREATER_THAN_EQUAL_TO(">=");

        private final String value;

        ComparisonOperator(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public DynamoExpressionBuilder(int prefixNumber) {
        String prefix = "t" + prefixNumber;
        names = new Alias("#" + prefix + "a");
        vals = new Alias(":" + prefix + "v");
        condVals = new Alias(":" + prefix + "condVal");
    }

    public void merge(DynamoExpressionBuilder dynamoExpressionBuilder) {
        this.addSection.addAll(dynamoExpressionBuilder.addSection);
        this.setSection.addAll(dynamoExpressionBuilder.setSection);
        this.removeSection.addAll(dynamoExpressionBuilder.removeSection);
        this.conditions.addAll(dynamoExpressionBuilder.conditions);
        nameMap.putAll(dynamoExpressionBuilder.nameMap);
        valueMap.putAll(dynamoExpressionBuilder.valueMap);
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            this.objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    public NameMap getNameMap() {
        return nameMap;
    }

    public ValueMap getValueMap() {
        return valueMap;
    }


    public DynamoExpressionBuilder incrementNumber(String parentField, String fieldName, Number amount) {
        String alias = vals.next();
        if (amount instanceof Integer) {
            valueMap = valueMap.withInt(alias, (Integer) amount);
        } else if (amount instanceof Long) {
            valueMap = valueMap.withLong(alias, (Long) amount);
        }
        addSection.add(String.format("%s %s", joinFields(parentField, fieldName), alias));
        return this;
    }

    public DynamoExpressionBuilder setValue(String parentField, String fieldName, Object value) {
        setSection.add(String.format("%s=%s", joinFields(parentField, fieldName), processValueAlias(vals, value)));
        return this;
    }

    public <V> DynamoExpressionBuilder setMultiValue(String parentField, String fieldName, Object collection, Class type) {
        setSection.add(String.format("%s=%s", joinFields(parentField, fieldName), processValueAlias(vals, collection, type)));
        return this;
    }

    public DynamoExpressionBuilder removeField(String parentField, String fieldName) {
        removeSection.add(String.format("%s", joinFields(parentField, fieldName)));
        return this;
    }

    public <N extends Number, T extends Object> DynamoExpressionBuilder updateMap(String parentField, String fieldName, Map<String, N> deltas, Map<String, T> updates, Collection<String> deletes, boolean clear, Class type) {
        if (clear) {
            setSection.add(String.format("%s=%s", joinFields(parentField, fieldName), processValueAlias(vals, Collections.emptyMap(), type)));
        }
        if (deltas != null) {
            processMapForAdd(parentField, fieldName, deltas);
        }
        if (updates != null) {
            processMapForUpdates(parentField, fieldName, updates);
        }
        if (deletes != null) {
            processDeletes(parentField, fieldName, deletes);
        }
        return this;
    }

    public String buildUpdateExpression() {
        List<String> updateExpressions = new ArrayList<>();
        if (!setSection.isEmpty()) {
            updateExpressions.add("SET " + Joiner.on(", ").join(setSection));
        }
        if (!addSection.isEmpty()) {
            updateExpressions.add("ADD " + Joiner.on(", ").join(addSection));
        }
        if (!removeSection.isEmpty()) {
            updateExpressions.add("REMOVE " + Joiner.on(", ").join(removeSection));
        }
        if (updateExpressions.isEmpty()) {
            return "";
        }
        return Joiner.on(" ").join(updateExpressions);
    }

    //////// Conditional Expression ////

    public DynamoExpressionBuilder addCheckFieldValueCondition(String parentField, String fieldName, Object value, ComparisonOperator op) {
        conditions.add(String.format("%s " + op.getValue() + " %s", joinFields(parentField, fieldName), processValueAlias(condVals, value)));
        return this;
    }

    public <T> DynamoExpressionBuilder addCheckMapValuesCondition(String parentField, String fieldName, Map<String, T> map, ComparisonOperator op) {
        for (Map.Entry<String, T> entry : map.entrySet()) {
            String valueAlias = condVals.next();
            valueMap.with(valueAlias, entry.getValue());
            conditions.add(joinFields(parentField, fieldName, entry.getKey()) + " " + op.getValue() + " " + valueAlias);
        }
        return this;
    }

    public DynamoExpressionBuilder addCheckAttributeInMapNotExistsCondition(String parentField, String fieldName, Collection<String> attributes) {
        for (String id : attributes) {
            conditions.add(String.format("attribute_not_exists(%s)", joinFields(parentField, fieldName, id)));
        }
        return this;
    }

    public DynamoExpressionBuilder addCheckAttributeInMapExistsCondition(String parentField, String fieldName, Collection<String> attributes) {
        for (String id : attributes) {
            conditions.add(String.format("attribute_exists(%s)", joinFields(parentField, fieldName, id)));
        }
        return this;
    }

    public DynamoExpressionBuilder addCheckAttributeSizeCondition(String parentField, String fieldName, Number value, ComparisonOperator op) {
        String valueAlias = condVals.next();
        valueMap.with(valueAlias, value);
        conditions.add("size(" + joinFields(parentField, fieldName) + ") " + op.getValue() + " " + valueAlias);
        return this;
    }

    public String buildConditionalExpression() {
        if (conditions.isEmpty()) {
            return "";
        }
        return Joiner.on(" AND ").join(conditions);
    }


    ///////////////

    private <N extends Number> void processMapForAdd(String parentField, String fieldName, Map<String, N> deltas) {
        for (Map.Entry<String, N> entry : deltas.entrySet()) {
            addSection.add(String.format("%s %s", joinFields(parentField, fieldName, entry.getKey()), processValueAlias(vals, entry.getValue())));
        }
    }

    private <T extends Object> void processMapForUpdates(String parentField, String fieldName, Map<String, T> updates) {
        for (Map.Entry<String, T> entry : updates.entrySet()) {
            setSection.add(String.format("%s=%s", joinFields(parentField, fieldName, entry.getKey()), processValueAlias(vals, entry.getValue())));
        }
    }

    private void processDeletes(String parentField, String fieldName, Collection<String> deletes) {
        for (String id : deletes) {
            removeSection.add(String.format("%s", joinFields(parentField, fieldName, id)));
        }
    }

    private String processValueAlias(Alias aliasGenerator, Object value) {
        return processValueAlias(aliasGenerator, value, null);
    }

    private String processValueAlias(Alias aliasGenerator, Object value, Class type) {
        ObjectMapper objectMapper = getObjectMapper();
        String alias = aliasGenerator.next();
        if (value instanceof Integer) {
            valueMap = valueMap.withInt(alias, (Integer) value);
        } else if (value instanceof Long) {
            valueMap = valueMap.withLong(alias, (Long) value);
        } else if (value instanceof String) {
            valueMap = valueMap.withString(alias, (String) value);
        } else if (value instanceof Map) {
            if (type == null) {
                throw new IllegalArgumentException("Must provide the object type for Map");
            } else {
                if (!SUPPORTED_JAVA_TYPES.contains(type.getName())) {
                    valueMap = valueMap.withMap(alias, objectMapper.convertValue(value, new TypeReference<Map<String, Object>>() {
                    }));
                } else {
                    valueMap = valueMap.withMap(alias, (Map) value);
                }
            }
        } else if (value instanceof Set) {
            if (type == null) {
                throw new IllegalArgumentException("Must provide the object type for Set");
            }
            if (type.getName().equals("java.lang.Number") || type.getName().equals("java.lang.Integer") || type.getName().equals("java.lang.Long")) {
                valueMap = valueMap.withNumberSet(alias, toBigDecimalSet((Set) value));
            } else if (type.getName().equals("java.lang.BigDecimal"))
                valueMap = valueMap.withNumberSet(alias, (Set) value);
            else if (type.getName().equals("java.lang.String")) {
                if (((Set) value).size() == 0) {
                    valueMap = valueMap.withList(alias, Collections.emptyList());
                } else {
                    valueMap = valueMap.withStringSet(alias, (Set) value);
                }
            } else {
                valueMap = valueMap.withStringSet(alias, toJsonStringSet((Set) value));
            }
        } else if (value instanceof List) {
            if (type == null) {
                throw new IllegalArgumentException("Must provide the object type for List");
            }
            if (SUPPORTED_JAVA_TYPES.contains(type.getName())) {
                valueMap = valueMap.withList(alias, (List) value);
            } else {
                List<String> values = new ArrayList<>();
                for (Object object : (List) value) {
                    values.add(objectMapper.convertValue(object, new TypeReference<Map<String, Object>>() {
                    }));
                }
                valueMap = valueMap.withList(alias, values);
            }
        } else {
            valueMap = valueMap.with(alias, objectMapper.convertValue(value, Object.class));
        }
        return alias;
    }

    private String joinFields(String... fields) {
        // if the parent field is null, then shift all fields down a position
        if (fields[0] == null) {
            fields[0] = fields[1];
            if (fields.length == 3) {
                fields = new String[]{fields[1], fields[2]};
            } else {
                fields = new String[]{fields[1]};
            }
        }
        for (int index = 0; index < fields.length; index++) {
            if (fields[index] != null) {
                String alias = nameMap.get(fields[index]);
                if (alias == null) {
                    alias = names.next();
                    nameMap = nameMap.with(alias, fields[index]);
                }
                fields[index] = alias;
            }

        }
        return Joiner.on(".").join(fields);
    }

    private Set<String> toJsonStringSet(Set<Object> objects) {
        ObjectMapper objectMapper = getObjectMapper();
        Set<String> results = new HashSet<>();
        for (Object object : objects) {
            results.add(objectMapper.convertValue(object, new TypeReference<Map<String, Object>>() {
            }));
        }
        return results;
    }

    private static BigDecimal toBigDecimal(Number n) {
        if (n instanceof BigDecimal)
            return (BigDecimal) n;
        return new BigDecimal(n.toString());
    }

    private static Set<BigDecimal> toBigDecimalSet(Number... val) {
        Set<BigDecimal> set = new LinkedHashSet<BigDecimal>(val.length);
        for (Number n : val)
            set.add(toBigDecimal(n));
        return set;
    }

    private static Set<BigDecimal> toBigDecimalSet(Set<Number> vals) {
        Set<BigDecimal> set = new LinkedHashSet<BigDecimal>(vals.size());
        for (Number n : vals)
            set.add(toBigDecimal(n));
        return set;
    }

}
