

package com.n3twork.dynamap;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.test.ExampleDocument;
import com.n3twork.dynamap.test.NestedType;
import com.n3twork.dynamap.test.NestedTypeUpdates;
import com.sun.xml.internal.bind.annotation.OverrideAnnotationOf;

public class ExampleDocumentUpdates implements ExampleDocument, Updates<ExampleDocument> {


    private final ExampleDocument currentExampledocument;
    private final String hashKeyValue;
    private final Object rangeKeyValue;

    private String exampleId;
    private Integer sequence;
    private Integer sequenceDelta;
    private NestedType nestedObject;
    private NestedTypeUpdates nestedObjectUpdates;
    private Map<String,Long> mapOfLong;
    private Set<String> mapOfLongDeletes = new HashSet();
    private Map<String,Long> mapOfLongSets = new HashMap();
    private Map<String,Long> mapOfLongDeltas = new HashMap();
    private Map<String,com.n3twork.dynamap.CustomType> mapOfCustomType;
    private Set<String> mapOfCustomTypeDeletes = new HashSet();
    private Map<String,com.n3twork.dynamap.CustomType> mapOfCustomTypeSets = new HashMap();
    private String alias;

    public ExampleDocumentUpdates(ExampleDocument currentExampledocument, String hashKeyValue, Object rangeKeyValue) {
        this.currentExampledocument = currentExampledocument;
        this.hashKeyValue = hashKeyValue;
        this.rangeKeyValue = rangeKeyValue;
    }

    public ExampleDocumentUpdates(ExampleDocument currentExampledocument, String hashKeyValue) {
        this(currentExampledocument, hashKeyValue, null);
    }

    @Override
    public String getTableName() {
        return "Example";
    }

    @Override
    public String getHashKeyValue() {
        return hashKeyValue;
    }

    @Override
    public Object getRangeKeyValue() {
        return rangeKeyValue;
    }

    ////// ExampleDocument interface methods //////

    @Override
    public String getExampleId() {
        return this.exampleId == null ? currentExampledocument.getExampleId() : this.exampleId;
    }

    @Override
    public Integer getSequence() {
        return MergeUtil.getLatestNumericValue(currentExampledocument.getSequence(), sequenceDelta, sequence);
    }

    @Override
    public NestedType getNestedObject() {
        return this.nestedObject == null ? currentExampledocument.getNestedObject() : this.nestedObject;
    }


    @Override
    public Set<String> getMapOfLongIds() {
        return MergeUtil.mergeUpdatesAndDeletes(currentExampledocument.getMapOfLongIds(), mapOfLongDeltas.keySet(), mapOfLongSets.keySet(), mapOfLongDeletes);
    }
    @Override
    public Long getMapOfLongValue(String id) {
        return MergeUtil.getLatestNumericValue(id, currentExampledocument.getMapOfLongValue(id), mapOfLongDeltas, mapOfLongSets);
    }



    @Override
    public Set<String> getMapOfCustomTypeIds() {
        return MergeUtil.mergeUpdatesAndDeletes(currentExampledocument.getMapOfCustomTypeIds(), null, mapOfCustomTypeSets.keySet(), mapOfCustomTypeDeletes);
    }
    @Override
    public com.n3twork.dynamap.CustomType getMapOfCustomTypeValue(String id) {
        return MergeUtil.getLatestValue(id, currentExampledocument.getMapOfCustomTypeValue(id), mapOfCustomTypeSets, mapOfCustomTypeDeletes);
    }


    @Override
    public String getAlias() {
        return this.alias == null ? currentExampledocument.getAlias() : this.alias;
    }


    /////// Mutator methods ///////////////////////

    public ExampleDocumentUpdates setExampleId(String value) {
        this.exampleId = value;
        return this;
    }
    public ExampleDocumentUpdates setSequence(Integer value) {
        this.sequence = value;
        return this;
    }
    public ExampleDocumentUpdates incrementSequence(Integer amount) {
        sequenceDelta = (sequenceDelta == null ? 0 : sequenceDelta) + amount;
        return this;
    }
    public ExampleDocumentUpdates setNestedObject(NestedType value) {
        if (this.nestedObjectUpdates != null) {
            throw new IllegalStateException("nestedObjectUpdates should not be set");
        }
        this.nestedObject = value;
        return this;
    }
    public ExampleDocumentUpdates setNestedObjectUpdates(NestedTypeUpdates value) {
        if (nestedObject != null) {
            throw new IllegalStateException("nestedObject should not be set");
        }
        this.nestedObjectUpdates = value;
        return this;
    }
    public ExampleDocumentUpdates incrementMapOfLongAmount(String id, Long amount) {
        mapOfLongDeltas.put(id, mapOfLongDeltas.getOrDefault(id, 0L) + amount);
        return this;
    }
    public ExampleDocumentUpdates setMapOfLongValue(String id, Long value) {
        mapOfLongSets.put(id, value);
        return this;
    }
    public ExampleDocumentUpdates deleteMapOfLongValue(String id) {
        mapOfLongDeletes.remove(id);
        return this;
    }
    public ExampleDocumentUpdates setMapOfCustomTypeValue(String id, com.n3twork.dynamap.CustomType value) {
        mapOfCustomTypeSets.put(id, value);
        return this;
    }
    public ExampleDocumentUpdates deleteMapOfCustomTypeValue(String id) {
        mapOfCustomTypeDeletes.remove(id);
        return this;
    }
    public ExampleDocumentUpdates setAlias(String value) {
        this.alias = value;
        return this;
    }

    //////////////// Updates Interface Methods //////////

    @Override
    public DynamoExpressionBuilder getUpdateExpression(ObjectMapper objectMapper) {
        DynamoExpressionBuilder expression = new DynamoExpressionBuilder(objectMapper);
        addUpdateExpression(expression);
        return expression;
    }

    @Override
    public void addUpdateExpression(DynamoExpressionBuilder expressionBuilder) {

        String parentDynamoFieldName = null;



            if (exampleId != null) {
                expressionBuilder.setValue(parentDynamoFieldName, "id", exampleId);
            }
            if (sequence != null) {
                expressionBuilder.setValue(parentDynamoFieldName, "seq", sequence);
            }
            else if (sequenceDelta != null) {
                expressionBuilder.incrementNumber(parentDynamoFieldName, "seq", sequenceDelta);
            }
            if (nestedObject != null) {
                expressionBuilder.setValue(parentDynamoFieldName, "nested", nestedObject);
            } else if (nestedObjectUpdates != null) {
                nestedObjectUpdates.addUpdateExpression(expressionBuilder);
            }

        expressionBuilder.updateMap(parentDynamoFieldName, "mol", mapOfLongDeltas, mapOfLongSets, mapOfLongDeletes);

        expressionBuilder.updateMap(parentDynamoFieldName, "mct", null, mapOfCustomTypeSets, mapOfCustomTypeDeletes);

            if (alias != null) {
                expressionBuilder.setValue(parentDynamoFieldName, "alias", alias);
            }
    }

    @Override
    public void addConditionalExpression(DynamoExpressionBuilder expression) {
        expression.addCheckFieldValueCondition(null, "schemaVersion", ExampleDocument.SCHEMA_VERSION);
    }
}