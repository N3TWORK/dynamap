<#--
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
-->

<#include "common.ftl">

package ${package};

<#list imports as import>
import ${import};
</#list>

import java.util.*;
import com.google.common.collect.ImmutableMap;
import com.n3twork.dynamap.*;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

public class ${updatesName} implements ${type.name}, Updates<${type.name}> {


    private final ${type.name} ${currentState};
    private final String hashKeyValue;
    private final Object rangeKeyValue;
<#if isRoot && optimisticLocking>
    private final Integer _revision;
</#if>
    private boolean pendingUpdates = false;

<#list type.fields as field>
    private <@field_type field=field /> ${field.name};
    <#if field.isGeneratedType()>
    private ${field.type}Updates ${field.name}Updates;
    </#if>

    <#if field.multiValue??>
    private boolean ${field.name}Clear = false;
    <#if field.multiValue == 'LIST'>
    private <@field_type field=field /> ${field.name}Adds = new ArrayList();
    </#if>
    <#if field.multiValue == 'SET'>
    private <@field_type field=field /> ${field.name}Deletes = new HashSet();
    private <@field_type field=field /> ${field.name}Sets = new HashSet();
    </#if>
    <#if field.multiValue == 'MAP'>
    private Set<String> ${field.name}Deletes = new HashSet();
    private <@field_type field=field /> ${field.name}Sets = new HashMap();
    </#if>
    <#if field.multiValue == 'MAP' && field.isNumber()>
    private <@field_type field=field /> ${field.name}Deltas = new HashMap();
    </#if>
    <#elseif field.isNumber()>
    private ${field.type} ${field.name}Delta;
    </#if>
</#list>
    private final boolean disableOptimisticLocking;

    public ${updatesName}(${type.name} ${currentState}, String hashKeyValue, <#if tableDefinition.rangeKey??>Object rangeKeyValue,</#if> boolean disableOptimisticLocking) {
        this.${currentState} = ${currentState};
        this.hashKeyValue = hashKeyValue;
        <#if tableDefinition.rangeKey??>
        this.rangeKeyValue = rangeKeyValue;
        <#else>
        this.rangeKeyValue = null;
        </#if>
        this.disableOptimisticLocking = disableOptimisticLocking;
<#if isRoot && optimisticLocking>
        this._revision = ${currentState}.getRevision();
</#if>
    }

    public ${updatesName}(${type.name} ${currentState}, String hashKeyValue<#if tableDefinition.rangeKey??>,Object rangeKeyValue</#if>) {
        this(${currentState}, hashKeyValue, <#if tableDefinition.rangeKey??>rangeKeyValue,</#if> false);
    }

    @Override
    public String getTableName() {
        return "${tableName}";
    }

    @Override
    public String getHashKeyValue() {
        return hashKeyValue;
    }

    @Override
    public Object getRangeKeyValue() {
        return rangeKeyValue;
    }

    ////// ${type.name} interface methods //////

    public ${type.name} getCurrentState() {
        return ${currentState};
    }

<#list type.fields as field>
    <#if field.multiValue??>

    <#if field.multiValue == 'MAP'>
    @Override
    public Set<String> get${field.name?cap_first}Ids() {
        if (${field.name} != null) {
            return ${field.name}.keySet();
        }
        <#if field.isNumber()>
            <#assign deltas>${field.name}Deltas.keySet()</#assign>
        <#else>
            <#assign deltas>null</#assign>
        </#if>
        return MergeUtil.mergeUpdatesAndDeletes(${currentState}.get${field.name?cap_first}Ids(), ${deltas}, ${field.name}Sets.keySet(), ${field.name}Deletes, ${field.name}Clear);
    }
    @Override
    public ${field.type} get${field.name?cap_first}Value(String id) {
        if (${field.name} != null) {
          <#if field.isUseDefaultForNulls()>
            return ${field.name}.getOrDefault(id, ${field.defaultValue});
          <#else>
            return ${field.name}.get(id);
          </#if>
        }
        <#if field.isNumber()>
        ${field.type} value = MergeUtil.getLatestNumericValue(id, ${currentState}.get${field.name?cap_first}Value(id), ${field.name}Deltas, ${field.name}Sets, ${field.name}Clear);
        <#else>
        ${field.type} value = MergeUtil.getLatestValue(id, ${currentState}.get${field.name?cap_first}Value(id), ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear);
        </#if>
        <#if field.isUseDefaultForNulls()>
        if (value == null) {
            return ${field.defaultValue};
        }
        </#if>
        return value;
    }
    @Override
    public Map<String,${field.type}> get${field.name?cap_first}() {
        if (${field.name} != null) {
            return ${field.name};
        }
        if (${field.name}Clear) {
            return Collections.emptyMap();
        }
        <#if field.isNumber()>
        if ( ${field.name}Deltas.size() > 0 || ${field.name}Deletes.size() > 0 || ${field.name}Sets.size() > 0) {
            Map<String, ${field.type}> allItems = new HashMap<>();
            for (String id : get${field.name?cap_first}Ids()) {
                allItems.put(id, get${field.name?cap_first}Value(id));
            }
            return allItems;
        }
        return ${currentState}.get${field.name?cap_first}();
        <#else>
        return MergeUtil.mergeUpdatesAndDeletes(${currentState}.get${field.name?cap_first}(), ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear);
        </#if>
    }
    <#elseif field.multiValue == 'LIST'>
    @Override
    public List<${field.type}> get${field.name?cap_first}() {
        if (${field.name} != null) {
            return ${field.name};
        }
        return MergeUtil.mergeAdds(${currentState}.get${field.name?cap_first}(), ${field.name}Adds, ${field.name}Clear);
    }
    <#elseif field.multiValue == 'SET'>
    @Override
    public Set<${field.type}> get${field.name?cap_first}() {
        if (${field.name} != null) {
            return ${field.name};
        }
        return MergeUtil.mergeUpdatesAndDeletes(${currentState}.get${field.name?cap_first}(), null, ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear);
    }
    </#if>
    <#else>
    @Override
    public ${field.type} get${field.name?cap_first}() {
        <#if field.isNumber()>
        return MergeUtil.getLatestNumericValue(${currentState}.get${field.name?cap_first}(), ${field.name}Delta, ${field.name});
        <#else>
        return this.${field.name} == null ? ${currentState}.get${field.name?cap_first}() : this.${field.name};
        </#if>
    }
    </#if>

</#list>
<#if isRoot && optimisticLocking>
    @Override
    public Integer getRevision() {
        return this._revision == null ? ${currentState}.getRevision() : this._revision;
    }
</#if>

    /////// Mutator methods ///////////////////////

<#list type.fields as field>
  <#if field.multiValue??>
    public ${updatesName} clear${field.name?cap_first}() {
        ${field.name}Clear = true;
        pendingUpdates = true;
        return this;
    }
  </#if>
    <#if field.multiValue! == 'MAP'>
       public ${updatesName} set${field.name?cap_first}(Map<String,${field.type}> value) {
            this.${field.name} = value;
            pendingUpdates = true;
            return this;
        }
        <#if field.isNumber()>
    public ${updatesName} increment${field.name?cap_first}Amount(String id, ${field.type} amount) {
        ${field.name}Deltas.put(id, ${field.name}Deltas.getOrDefault(id, ${field.defaultValue}) + amount);
        pendingUpdates = true;
        return this;
    }
    public ${updatesName} decrement${field.name?cap_first}Amount(String id, ${field.type} amount) {
        ${field.name}Deltas.put(id, ${field.name}Deltas.getOrDefault(id, ${field.defaultValue}) - amount);
        pendingUpdates = true;
        return this;
    }
        </#if>
    public ${updatesName} set${field.name?cap_first}Value(String id, ${field.type} value) {
        ${field.name}Sets.put(id, value);
        pendingUpdates = true;
        return this;
    }
    public ${updatesName} delete${field.name?cap_first}Value(String id) {
        ${field.name}Deletes.remove(id);
        pendingUpdates = true;
        return this;
    }
    <#elseif field.multiValue! == 'LIST'>
    public ${updatesName} add${field.name?cap_first}Value(${field.type} value) {
        ${field.name}Adds.add(value);
        pendingUpdates = true;
        return this;
    }
    public ${updatesName} set${field.name?cap_first}(List<${field.type}> list) {
        this.${field.name} = list;
        pendingUpdates = true;
        return this;
    }
    <#elseif field.multiValue! == 'SET'>
    public ${updatesName} set${field.name?cap_first}(Set<${field.type}> value) {
        this.${field.name} = value;
        pendingUpdates = true;
        return this;
    }
    public ${updatesName} setValue${field.name?cap_first}Value(${field.type} value) {
        ${field.name}Sets.add(value);
        pendingUpdates = true;
        return this;
    }
    public ${updatesName} delete${field.name?cap_first}Value(${field.type} value) {
        ${field.name}Deletes.remove(value);
        pendingUpdates = true;
        return this;
    }
    <#else>
    public ${updatesName} set${field.name?cap_first}(${field.type} value) {
        this.${field.name} = value;
        pendingUpdates = true;
        return this;
    }
    <#if field.isNumber()>
    public ${updatesName} increment${field.name?cap_first}(${field.type} amount) {
        ${field.name}Delta = (${field.name}Delta == null ? ${field.defaultValue} : ${field.name}Delta) + amount;
        pendingUpdates = true;
        return this;
    }
    public ${updatesName} decrement${field.name?cap_first}(${field.type} amount) {
        ${field.name}Delta = (${field.name}Delta == null ? ${field.defaultValue} : ${field.name}Delta) - amount;
        pendingUpdates = true;
        return this;
    }
    </#if>
    </#if>
</#list>

    //////////////// Nested Updates ////////////////
    <#list type.fields as field>
    <#if field.isGeneratedType()>
    public ${field.type}Updates get${field.name?cap_first}Updates() {
        return this.${field.name}Updates;
    }

    public ${updatesName} set${field.name?cap_first}Updates(${field.type}Updates value) {
        if (this.${field.name} != null) {
            throw new IllegalStateException("Nested property: ${field.name}, should not be set when passing its Updates object");
        }
        this.${field.name}Updates = value;
        pendingUpdates = true;
        return this;
    }
    </#if>
    </#list>

    //////////////// Updates Interface Methods //////////

    @Override
    public boolean pendingUpdates() {
        return pendingUpdates;
    }

    @Override
    public DynamoExpressionBuilder getUpdateExpression(ObjectMapper objectMapper) {
        DynamoExpressionBuilder expression = new DynamoExpressionBuilder(objectMapper);
        addUpdateExpression(expression);
        return expression;
    }

    @Override
    public void addUpdateExpression(DynamoExpressionBuilder expression) {

        String parentDynamoFieldName = <#if isRoot>null;<#else>"${parentFieldName}";</#if>
<#if isRoot && optimisticLocking>
        if (!disableOptimisticLocking) {
            expression.incrementNumber(parentDynamoFieldName, "${revisionFieldName}", 1, true);
        }
</#if>

    <#list type.persistedFields as field>
        <#if field.multiValue! == 'MAP'>
                if (${field.name} != null) {
                    expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", get${field.name?cap_first}(), ${field.type}.class);
                }
                else {
            <#if field.isReplace()>
                expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", get${field.name?cap_first}(), ${field.type}.class);
            <#else>
                <#if field.isNumber()>
                    expression.updateMap(parentDynamoFieldName, "${field.dynamoName}", ${field.name}Deltas, ${field.name}Sets, ${field.name}Deletes);
                <#else>
                    expression.updateMap(parentDynamoFieldName, "${field.dynamoName}", null, ${field.name}Sets, ${field.name}Deletes);
                </#if>
             </#if>
                }

        <#elseif field.multiValue! == 'LIST'>
            if (${field.name} != null) {
                expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", ${field.name}, ${field.type}.class);
            }
            else {
                for (Object value : ${field.name}Adds) {
                    expression.incrementNumber(parentDynamoFieldName, "${field.dynamoName}", (Number) value);
                }
            }
        <#elseif field.multiValue! == 'SET'>
            if (${field.name} != null) {
                expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", ${field.name}, ${field.type}.class);
            }
            else {
                for (Object value : ${field.name}Sets) {
                    expression.setValue(parentDynamoFieldName, "${field.dynamoName}", ${field.name}Sets);
                }
            }
        <#else>
            <#if field.isNumber()>
            if (${field.name} != null) {
                expression.setValue(parentDynamoFieldName, "${field.dynamoName}", ${field.name});
            }
            else if (${field.name}Delta != null) {
                expression.incrementNumber(parentDynamoFieldName, "${field.dynamoName}", ${field.name}Delta);
            }
            <#else>
            if (${field.name} != null) {
                expression.setValue(parentDynamoFieldName, "${field.dynamoName}", ${field.name});
            }
            <#if field.isGeneratedType()>
            else if (${field.name}Updates != null) {
                this.${field.name}Updates.addUpdateExpression(expression);
            }
            </#if>
            </#if>
        </#if>
    </#list>
    }

    @Override
    public void addConditionalExpression(DynamoExpressionBuilder expression) {
        expression.addCheckFieldValueCondition(null, "${schemaVersionFieldName}", ${rootType}.SCHEMA_VERSION, true);
<#if isRoot && optimisticLocking>
        if (!disableOptimisticLocking) {
            expression.addCheckFieldValueCondition(null, "${revisionFieldName}", ${currentState}.getRevision(), true);
        }
</#if>
    }

}