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
import com.n3twork.dynamap.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ${beanName} implements ${type.name}<#if isRoot>, DynamapRecordBean<${type.name}></#if> {

    <#list type.fields as field>
    <#if field.isSerialize()>
    @JsonProperty(${field.name?upper_case}_FIELD)
    </#if>
    private <@field_type field=field /> ${field.name};
    </#list>
    <#if isRoot>
    private int _schemaVersion;
    </#if>
    <#if isRoot && optimisticLocking>
    @JsonProperty(REVISION_FIELD)
    private Integer _revision;
    </#if>

    public ${beanName}() {
        this(<#list type.serializedFields as field>null<#sep>,</#list>
        <#if isRoot && optimisticLocking>,null</#if><#if isRoot>,null</#if>);
    }

    @JsonCreator
    public ${beanName}(
        <#list type.serializedFields as field>
        @JsonProperty(${field.name?upper_case}_FIELD) <#if field.generatedType>${field.type}Bean<#else><@field_type field=field /></#if> ${field.name}<#sep>,
        </#list><#if isRoot && optimisticLocking>,@JsonProperty(REVISION_FIELD) Integer _revision</#if>
            <#if isRoot>,@JsonProperty(SCHEMA_VERSION_FIELD) Integer _schemaVersion</#if>) {

    <#list type.fields as field>
        <#if field.isPersist()>
            <#if field.multiValue??>
            <#if field.multiValue == 'MAP'>
            this.${field.name} = ${field.name} == null ? Collections.emptyMap() : ${field.name};
            <#elseif field.multiValue == 'LIST'>
            this.${field.name} = ${field.name} == null ? Collections.emptyList() : ${field.name};
            <#elseif field.multiValue == 'SET'>
            this.${field.name} = ${field.name} == null ? Collections.emptySet() : ${field.name};
            </#if>
            <#else>
            this.${field.name} = ${field.name} == null ? ${field.defaultValue} : ${field.name};
            </#if>
        <#else>
           <#if field.multiValue??>
           <#if field.multiValue == 'MAP'>
                this.${field.name} = ${field.name} == null ? Collections.emptyMap() : ${field.name};
                <#elseif field.multiValue == 'LIST'>
                this.${field.name} = ${field.name} == null ? Collections.emptyList() : ${field.name};
                <#elseif field.multiValue == 'SET'>
                this.${field.name} = ${field.name} == null ? Collections.emptySet() : ${field.name};
           </#if>
           </#if>
        </#if>
    </#list>
        <#if isRoot && optimisticLocking>
            this._revision = _revision == null ? 0 : _revision;
        </#if>
        <#if isRoot>
            this._schemaVersion = _schemaVersion == null ? SCHEMA_VERSION : _schemaVersion;
        </#if>
    }

    public ${beanName}(${type.name} bean) {

    <#list type.fields as field>
        <#if field.multiValue?? && field.multiValue == 'MAP'>
        <#if field.multiValue == 'MAP'>
        this.${field.name} = new HashMap();
        for (String id : bean.get${field.name?cap_first}Ids()) {
            this.${field.name}.put(id, bean.get${field.name?cap_first}<@collection_item field=field />(id));
        }
        </#if>
        <#else>
        this.${field.name} = bean.get${field.name?cap_first}();
        </#if>
     </#list>
    <#if isRoot && optimisticLocking>
        this._revision = bean.getRevision();
    </#if>
   <#if isRoot>
        this._schemaVersion = bean.getDynamapSchemaVersion();
    </#if>
    }

    public static String getTableName() {
        return "${tableName}";
    }

    <#if tableDefinition.globalSecondaryIndexes??>
    public enum GlobalSecondaryIndex implements DynamapRecordBean.GlobalSecondaryIndexEnum {
        <#list tableDefinition.globalSecondaryIndexes as index>${index.indexName}("${index.indexName}")<#sep>, </#sep></#list>;

        private final String name;

        GlobalSecondaryIndex(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
    </#if>

    <#if isRoot>
    @Override
    public String getHashKeyValue() {
        return ${tableDefinition.hashKey};
    }

    public Object getRangeKeyValue() {
        <#if tableDefinition.rangeKey??>
        return ${tableDefinition.rangeKey};
        <#else>
        return null;
        </#if>
    }
    </#if>

    <#if type.hashCodeFields??>
    @Override
    public int hashCode() {
        int result = 0;
        <#list type.hashCodeFields as field>
        result = 31 * result + (${field} == null ? 0 : ${field}.hashCode());
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
        if (!${field}.equals(that.get${field?cap_first}())) return false;
        </#list>
        return true;
    }
    </#if>

    <#list type.fields as field>
    <#if !field.isSerialize()>
    @JsonIgnore
    </#if>
    @Override
    public <@field_type field=field /> get${field.name?cap_first}() {
        return this.${field.name};
    }
    public ${beanName} set${field.name?cap_first}(<@field_type field=field /> value) {
        this.${field.name} = value;
        return this;
    }
    <#if field.multiValue! == 'MAP'>
        @JsonIgnore
        public Set<String> get${field.name?cap_first}Ids() {
        return this.${field.name}.keySet();
        }
        @JsonIgnore
        public ${field.type} get${field.name?cap_first}<@collection_item field=field />(String id) {
            <#if field.useDefaultForNulls()>
            return this.${field.name}.getOrDefault(id, ${field.defaultValue});
            <#else>
            return this.${field.name}.get(id);
            </#if>
        }
    </#if>
    </#list>
    <#if isRoot && optimisticLocking>
    public Integer getRevision() {
        return this._revision;
    }
    public ${beanName} setRevision(Integer value) {
        this._revision = value;
        return this;
    }
    </#if>

    <#if isRoot>
    @Override
    public int getDynamapSchemaVersion() {
        return this._schemaVersion;
    }
    public ${beanName} setDynamapSchemaVersion(int schemaVersion) {
        this._schemaVersion = schemaVersion;
        return this;
    }
    </#if>
}