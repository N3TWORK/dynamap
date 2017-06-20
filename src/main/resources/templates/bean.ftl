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
    <#if field.persisted>
    @JsonProperty(${field.name?upper_case}_FIELD)
    </#if>
    private <@field_type field=field /> ${field.name};
    </#list>
    <#if isRoot && optimisticLocking>
    @JsonProperty(REVISION_FIELD)
    private Integer _revision;
    </#if>

    public ${beanName}() {
    }

<#if isRoot && optimisticLocking>
    public ${beanName}(
        <#list type.persistedFields as field>
            @JsonProperty(${field.name?upper_case}_FIELD) <#if field.generatedType>${field.type}Bean<#else><@field_type field=field /></#if> ${field.name}<#sep>,
        </#list>) {
        this(<#list type.persistedFields as field>${field.name}<#sep>,</#list>, null);
    }
</#if>

    @JsonCreator
    public ${beanName}(
        <#list type.persistedFields as field>
        @JsonProperty(${field.name?upper_case}_FIELD) <#if field.generatedType>${field.type}Bean<#else><@field_type field=field /></#if> ${field.name}<#sep>,
        </#list><#if isRoot && optimisticLocking>,
        @JsonProperty(REVISION_FIELD) Integer _revision</#if>) {

    <#list type.persistedFields as field>
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
    </#list>
    <#if isRoot && optimisticLocking>
        this._revision = _revision == null ? 0 : _revision;
    </#if>
    }

    public ${beanName}(${type.name} bean) {

    <#list type.fields as field>
        <#if field.multiValue?? && field.multiValue == 'MAP'>
        <#if field.multiValue == 'MAP'>
        this.${field.name} = new HashMap();
        for (String id : bean.get${field.name?cap_first}Ids()) {
            this.${field.name}.put(id, bean.get${field.name?cap_first}Value(id));
        }
        </#if>
        <#else>
        this.${field.name} = bean.get${field.name?cap_first}();
        </#if>
     </#list>
    <#if isRoot && optimisticLocking>
        this._revision = bean.getRevision();
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

    <#list type.fields as field>
    <#if !field.persisted>
    @JsonIgnore
    </#if>
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
        public ${field.type} get${field.name?cap_first}Value(String id) {
            <#if field.isUseDefaultForMap()>
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
}