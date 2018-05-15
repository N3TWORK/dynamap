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

public class ${updatesName} implements ${type.name}, <#if isRoot>Record</#if>Updates<${type.name}> {

    protected final DynamoExpressionBuilder expression = new DynamoExpressionBuilder(${typeSequence});
    protected boolean updatesApplied = false;
    protected final ${type.name} ${currentState};
    <#if isRoot>
    protected final String hashKeyValue;
    protected final Object rangeKeyValue;
    </#if>
<#if isRoot && optimisticLocking>
    protected final Integer _revision;
</#if>
    protected boolean persistedModified = false;
    protected boolean modified = false;

<#list type.fields as field>
    protected <@field_type field=field /> ${field.name};
    <#if field.isGeneratedType()>
    protected ${field.elementType}Updates ${field.name}Updates;
    </#if>
    protected boolean ${field.name}Modified = false;
    <#if field.isCollection()>
    protected boolean ${field.name}Clear = false;
    <#if field.type == 'List'>
    protected <@field_type field=field /> ${field.name}Adds = new ArrayList();
    </#if>
    <#if field.type == 'Set'>
    protected <@field_type field=field /> ${field.name}Deletes = new HashSet();
    protected <@field_type field=field /> ${field.name}Sets = new HashSet();
    </#if>
    <#if field.type == 'Map'>
    protected Set<String> ${field.name}Deletes = new HashSet();
    protected <@field_type field=field /> ${field.name}Sets = new HashMap();
    </#if>
    <#if field.type == 'Map' && field.isNumber()>
    protected <@field_type field=field /> ${field.name}Deltas = new HashMap();
    </#if>
    <#elseif field.isNumber()>
    protected ${field.elementType} ${field.name}Delta;
    </#if>
</#list>
    protected boolean disableOptimisticLocking;

    @Deprecated
    public ${updatesName}(${type.name} ${currentState}, String hashKeyValue, <#if tableDefinition.rangeKey??>Object rangeKeyValue,</#if> boolean disableOptimisticLocking) {
        this.${currentState} = ${currentState};
        <#if isRoot>
            this.hashKeyValue = hashKeyValue;
            <#if tableDefinition.rangeKey??>
            this.rangeKeyValue = rangeKeyValue;
            <#else>
            this.rangeKeyValue = null;
            </#if>
        </#if>
        this.disableOptimisticLocking = disableOptimisticLocking;
<#if isRoot && optimisticLocking>
        this._revision = ${currentState}.getRevision();
</#if>
    }

    <#if isRoot>
        ${updatesName}(${type.name} ${currentState}, String hashKeyValue<#if tableDefinition.rangeKey??>,Object rangeKeyValue</#if>) {
            this.${currentState} = ${currentState};
            this.hashKeyValue = hashKeyValue;
            <#if tableDefinition.rangeKey??>
            this.rangeKeyValue = rangeKeyValue;
            <#else>
            this.rangeKeyValue = null;
            </#if>
            <#if isRoot && optimisticLocking>
            this._revision = ${currentState}.getRevision();
            </#if>
        }

        public ${updatesName} createUpdates() {
            return new ${updatesName}(this, getHashKeyValue()<#if tableDefinition.rangeKey??>, getRangeKeyValue()</#if>);
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

        <#if optimisticLocking>
        public ${updatesName} setDisableOptimisticLocking(boolean disableOptimisticLocking) {
            this.disableOptimisticLocking = disableOptimisticLocking;
            return this;
        }
    </#if>

    <#else>

         ${updatesName}(${type.name} ${currentState}) {
            this.${currentState} = ${currentState};
         }

         public ${updatesName} createUpdates() {
            return new ${updatesName}(this);
         }

    </#if>


    ////// ${type.name} interface methods //////

    public ${type.name} getCurrentState() {
        return ${currentState};
    }

<#list type.fields as field>
    <#if field.isCollection()>

    <#if field.type == 'Map'>
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
        if (${field.name}Modified) {
            return MergeUtil.mergeUpdatesAndDeletes(${currentState}.get${field.name?cap_first}Ids(), ${deltas}, ${field.name}Sets.keySet(), ${field.name}Deletes, ${field.name}Clear);
        }
        else {
            return ${currentState}.get${field.name?cap_first}Ids();
        }
    }
    @Override
    public ${field.elementType} get${field.name?cap_first}<@collection_item field=field />(String id) {
        if (${field.name} != null) {
          <#if field.useDefaultForNulls()>
            return ${field.name}.getOrDefault(id, <@defaultValue field=field elementOnly=true />);
          <#else>
            return ${field.name}.get(id);
          </#if>
        }
        <#if field.isNumber()>
        ${field.elementType} value = MergeUtil.getLatestNumericValue(${field.elementType}.class, id, ${currentState}.get${field.name?cap_first}<@collection_item field=field />(id), ${field.name}Deltas, ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear);
        <#else>
        ${field.elementType} value = MergeUtil.getLatestValue(id, ${currentState}.get${field.name?cap_first}<@collection_item field=field />(id), ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear);
        </#if>
        <#if field.useDefaultForNulls()>
        if (value == null) {
            return <@defaultValue field=field elementOnly=true />;
        }
        </#if>
        return value;
    }
    @Override
    public Map<String,${field.elementType}> get${field.name?cap_first}() {
        if (${field.name} != null) {
            return ${field.name};
        }
        if (${field.name}Clear) {
            return Collections.emptyMap();
        }
        <#if field.isNumber()>
        if ( ${field.name}Deltas.size() > 0 || ${field.name}Deletes.size() > 0 || ${field.name}Sets.size() > 0) {
            Map<String, ${field.elementType}> allItems = new HashMap<>();
            for (String id : get${field.name?cap_first}Ids()) {
                allItems.put(id, get${field.name?cap_first}<@collection_item field=field />(id));
            }
            return allItems;
        }
        return ${currentState}.get${field.name?cap_first}();
        <#else>
        if (${field.name}Modified) {
            return MergeUtil.mergeUpdatesAndDeletes(${currentState}.get${field.name?cap_first}(), ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear);
        }
        else {
            return ${currentState}.get${field.name?cap_first}();
        }
        </#if>
    }
    <#if field.isNumber()>
    public Map<String,${field.elementType}> get${field.name?cap_first}Deltas() {
        return ${field.name}Deltas;
    }
    </#if>
    <#elseif field.type == 'List'>
    @Override
    public List<${field.elementType}> get${field.name?cap_first}() {
        if (${field.name} != null) {
            return ${field.name};
        }
        return MergeUtil.mergeAdds(${currentState}.get${field.name?cap_first}(), ${field.name}Adds, ${field.name}Clear);
    }
    <#elseif field.type == 'Set'>
    @Override
    public Set<${field.elementType}> get${field.name?cap_first}() {
        if (${field.name} != null) {
            return ${field.name};
        }
        if (${field.name}Modified) {
            return MergeUtil.mergeUpdatesAndDeletes(${currentState}.get${field.name?cap_first}(), null, ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear);
        }
        else {
            return ${currentState}.get${field.name?cap_first}();
        }
    }
    </#if>
    <#else>
    @Override
    public ${field.elementType} get${field.name?cap_first}() {
        <#if field.isNumber()>
        return MergeUtil.getLatestNumericValue(${field.elementType}.class, ${currentState}.get${field.name?cap_first}(), ${field.name}Delta, ${field.name});
        <#else>
        return this.${field.name} == null ? ${currentState}.get${field.name?cap_first}() : this.${field.name};
        </#if>
    }
    </#if>

    public boolean is${field.name?cap_first}Modified() {
        return this.${field.name}Modified;
    }

</#list>
<#if isRoot && optimisticLocking>
    @Override
    public Integer getRevision() {
        return this._revision == null ? ${currentState}.getRevision() : this._revision;
    }
</#if>
<#if tableDefinition.isEnableMigrations() && isRoot>
    @Override
    public int getDynamapSchemaVersion() {
        return ${currentState}.getDynamapSchemaVersion();
    }
</#if>

    <#if type.hashCodeFields??>
    @Override
    public int hashCode() {
        int result = 0;
        <#list type.hashCodeFields as field>
        result = 31 * result + (get${field?cap_first}() == null ? 0 : get${field?cap_first}().hashCode());
        </#list>
        return result;
    }
    </#if>

    <#if type.equalsFields??>
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ${type.name} that = (${type.name}) o;

        <#list type.equalsFields as field>
        if (!get${field?cap_first}().equals(that.get${field?cap_first}())) return false;
        </#list>
        return true;
    }
    </#if>




    /////// Mutator methods ///////////////////////

<#list type.fields as field>
  <#if field.isCollection()>
    <#if field.useDeltas()>
    public ${updatesName} clear${field.name?cap_first}() {
        ${field.name}Clear = true;
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    </#if>
  </#if>
    <#if field.type == 'Map'>
    <#if field.useDeltas()>
        <#if field.isNumber()>
    public ${updatesName} increment${field.name?cap_first}Amount(String id, ${field.elementType} amount) {
        ${field.name}Deltas.put(id, ${field.name}Deltas.getOrDefault(id, <@numberSuffix field 0 />) + amount);
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    public ${updatesName} decrement${field.name?cap_first}Amount(String id, ${field.elementType} amount) {
        ${field.name}Deltas.put(id, ${field.name}Deltas.getOrDefault(id, <@numberSuffix field 0 />) - amount);
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
        </#if>
    public ${updatesName} set${field.name?cap_first}<@collection_item field=field />(String id, ${field.elementType} value) {
        ${field.name}Sets.put(id, value);
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    public ${updatesName} set${field.name?cap_first}<@collection_item field=field />(String id, ${field.elementType} value, boolean override) {
        ${field.name}Sets.put(id, value);
        if (override) {
            ${field.name}Deletes.remove(id);
        }
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    public ${updatesName} delete${field.name?cap_first}<@collection_item field=field />(String id) {
        ${field.name}Deletes.add(id);
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }

    <#else>
       public ${updatesName} set${field.name?cap_first}(Map<String,${field.elementType}> value) {
                this.${field.name} = value;
                modified = true;
                ${field.name}Modified = true;
                return this;
        }
    </#if>

    <#elseif field.type == 'List'>
    public ${updatesName} add${field.name?cap_first}<@collection_item field=field />(${field.elementType} value) {
        ${field.name}Adds.add(value);
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    public ${updatesName} set${field.name?cap_first}(List<${field.elementType}> list) {
        this.${field.name} = list;
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    <#elseif field.type == 'Set'>
    <#if field.useDeltas()>
    public ${updatesName} set${field.name?cap_first}<@collection_item field=field />(${field.elementType} value) {
        ${field.name}Sets.add(value);
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    public ${updatesName} delete${field.name?cap_first}<@collection_item field=field />(${field.elementType} value) {
        ${field.name}Deletes.remove(value);
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    <#else>
    public ${updatesName} set${field.name?cap_first}(Set<${field.elementType}> value) {
        this.${field.name} = value;
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    </#if>
    <#else>
    public ${updatesName} set${field.name?cap_first}(${field.elementType} value) {
        this.${field.name} = value;
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    <#if field.isNumber()>
    public ${updatesName} increment${field.name?cap_first}(${field.elementType} amount) {
        ${field.name}Delta = (${field.name}Delta == null ? amount: ${field.name}Delta) + amount;
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    public ${updatesName} decrement${field.name?cap_first}(${field.elementType} amount) {
        ${field.name}Delta = (${field.name}Delta == null ? amount : ${field.name}Delta) - amount;
        modified = true;
        ${field.name}Modified = true;
        <@persisted_modified field/>
        return this;
    }
    </#if>
    </#if>
</#list>

    /////////// Expose Modifications /////////////

<#list type.fields as field>
    <#if field.isCollection()>
        <#if field.type == 'List'>
        public <@field_type field=field /> ${field.name}Adds() {
            return ${field.name}Adds;
        }
        </#if>
        <#if field.type == 'Set'>
        public <@field_type field=field /> ${field.name}Deletes() {
            return ${field.name}Deletes;
        }
        public <@field_type field=field /> ${field.name}Sets() {
            return ${field.name}Sets;
        }
        </#if>
        <#if field.type == 'Map'>
        public Set<String> ${field.name}Deletes() {
            return ${field.name}Deletes;
        }
        public <@field_type field=field /> ${field.name}Sets() {
            return ${field.name}Sets;
        }
        </#if>
        <#if field.type == 'Map' && field.isNumber()>
        public <@field_type field=field /> ${field.name}Deltas() {
            return ${field.name}Deltas;
        }
        </#if>
    </#if>
</#list>



    //////////////// Nested Updates ////////////////
    <#list type.fields as field>
    <#if field.isGeneratedType()>
    public ${field.elementType}Updates get${field.name?cap_first}Updates() {
        return this.${field.name}Updates;
    }

    public ${updatesName} set${field.name?cap_first}Updates(${field.elementType}Updates value) {
        if (this.${field.name} != null) {
            throw new IllegalStateException("Nested property: ${field.name}, should not be set when passing its Updates object");
        }
        this.${field.name}Updates = value;
        modified = true;
        return this;
    }
    </#if>
    </#list>

    //////////////// Updates Interface Methods //////////

    @Override
    public DynamoExpressionBuilder getExpressionBuilder() {
        return expression;
    }

    @Override
    public boolean isPersistedModified() {
        return persistedModified;
    }

   @Override
    public boolean isModified() {
        return modified;
    }

    @Override
    public void processUpdateExpression() {

        if (updatesApplied) {
            throw new IllegalStateException("Updates have already been applied. A new Updates object must be created");
        }
        updatesApplied = true;

        String parentDynamoFieldName = <#if isRoot>null;<#else>"${parentFieldName}";</#if>
<#if isRoot && optimisticLocking>
        if (!disableOptimisticLocking) {
            expression.incrementNumber(parentDynamoFieldName, "${revisionFieldName}", 1);
        }
</#if>

    <#list type.persistedFields as field>
        <#if field.type == 'Map'>
            <#if field.useDeltas()>
            <#if field.isReplace()>
                expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", get${field.name?cap_first}(), ${field.elementType}.class);
            <#else>
                <#if field.isNumber()>
                    expression.updateMap(parentDynamoFieldName, "${field.dynamoName}", ${field.name}Deltas, ${currentState}.get${field.name?cap_first}Ids(), <@defaultValue field=field elementOnly=true />, ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear, ${field.elementType}.class);
                <#else>
                    expression.updateMap(parentDynamoFieldName, "${field.dynamoName}", null, null, null, ${field.name}Sets, ${field.name}Deletes, ${field.name}Clear, ${field.elementType}.class);
                </#if>
             </#if>
            <#else>
                expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", get${field.name?cap_first}(), ${field.elementType}.class);
            </#if>

        <#elseif field.type == 'List'>
            <#if field.useDeltas()>
                for (Object value : ${field.name}Adds) {
                    expression.incrementNumber(parentDynamoFieldName, "${field.dynamoName}", (Number) value);
                }
            <#else>
                expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", get${field.name?cap_first}(), ${field.elementType}.class);
            </#if>

        <#elseif field.type == 'Set'>
            <#if field.useDeltas()>
                expression.addSetValuesToSet(parentDynamoFieldName, "${field.dynamoName}", ${field.name}Sets, ${field.elementType}.class);
            <#else>
                expression.setMultiValue(parentDynamoFieldName, "${field.dynamoName}", get${field.name?cap_first}(), ${field.elementType}.class);
            </#if>
        <#else>
            <#if field.isNumber()>
            if (${field.name} != null) {
                expression.setValue(parentDynamoFieldName, "${field.dynamoName}", ${field.name});
            }
            else if (${field.name}Delta != null) {
                expression.incrementNumber(parentDynamoFieldName, "${field.dynamoName}", ${field.name}Delta);
            }
            <#else>
            if (${field.name}Modified == true) {
                if (${field.name} != null) {
                    expression.setValue(parentDynamoFieldName, "${field.dynamoName}", ${field.name});
                }
                else {
                    expression.removeField(parentDynamoFieldName, "${field.dynamoName}");
                }
            }
            <#if field.isGeneratedType()>
            else if (${field.name}Updates != null) {
                DynamoExpressionBuilder nestedExpression = this.${field.name}Updates.getExpressionBuilder();
                nestedExpression.setObjectMapper(expression.getObjectMapper());
                this.${field.name}Updates.processUpdateExpression();
                expression.merge(this.${field.name}Updates.getExpressionBuilder());
            }
            </#if>
            </#if>
        </#if>
    </#list>

    // Conditional expression
    expression.addCheckFieldValueCondition(null, "${schemaVersionFieldName}", ${rootType}.SCHEMA_VERSION, DynamoExpressionBuilder.ComparisonOperator.EQUALS);
    <#if isRoot && optimisticLocking>
            if (!disableOptimisticLocking) {
                expression.addCheckFieldValueCondition(null, "${revisionFieldName}", ${currentState}.getRevision(), DynamoExpressionBuilder.ComparisonOperator.EQUALS);
            }
    </#if>

    }

}