<#--
    Copyright 2019 N3TWORK INC

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

import java.util.*;
import com.n3twork.dynamap.*;
<#list imports as import>
import ${import};
</#list>

public class ${type.name}UpdateResultBean implements ${type.name}UpdateResult {

private ${type.name}Updates previous;
private ${type.name}Bean updated;
private ${type.name}Bean newBean = new ${type.name}Bean(); // DynamoDB returns partial maps for UPDATE_NEW return values so need to keep track of old and new in this bean

<#list type.fields as field>
<#if field.isGeneratedType()>
    private ${field.elementType}UpdateResult ${field.name}UpdateResult;
</#if>
</#list>

public ${type.name}UpdateResultBean(${type.name}Updates previous, ${type.name}Bean updated) {
    this.previous = previous;
    this.updated = updated;
    <#list type.fields as field>
    <#if field.isGeneratedType()>
        if (!(previous.get${field.name?cap_first}Updates() == null)) {
            this.${field.name}UpdateResult = new ${field.elementType}UpdateResultBean(previous.get${field.name?cap_first}Updates(),updated == null ? null : (${field.elementType}Bean) updated.get${field.name?cap_first}());
        }
    </#if>
    <#if field.type == 'Map' && field.persist>
    if (updated != null && updated.get${field.name?cap_first}() != null && previous.get${field.name?cap_first}() != null) {
        Map map = new HashMap(previous.get${field.name?cap_first}());
        map.putAll(updated.get${field.name?cap_first}());
        newBean.set${field.name?cap_first}(map);
    }
    else if (updated != null && previous.get${field.name?cap_first}() == null) {
        newBean.set${field.name?cap_first}(updated.get${field.name?cap_first}());
    }
    else {
        newBean.set${field.name?cap_first}(previous.get${field.name?cap_first}());
    }
    </#if>
    </#list>
}

<#if tableDefinition.isEnableMigrations() && isRoot>
    public Integer getDynamapSchemaVersion() {
        return updated == null || updated.getDynamapSchemaVersion() == null ? previous.getDynamapSchemaVersion() : updated.getDynamapSchemaVersion();
    }
</#if>

<#if isRoot>
    private String getHashKeyValue() {
        return updated == null || updated.getHashKeyValue() == null ? previous.getHashKeyValue() : updated.getHashKeyValue();
    }
    private Object getRangeKeyValue() {
        return updated == null || updated.getRangeKeyValue() == null ? previous.getRangeKeyValue() : updated.getRangeKeyValue();
    }
</#if>


@Override
public ${updatesName} createUpdates() {
 <#if isRoot>
 return new ${updatesName}(this, getHashKeyValue()<#if tableDefinition.rangeKey??>, getRangeKeyValue()</#if>);
 <#else>
 return new ${updatesName}(this);
 </#if>
}


<#list type.fields as field>
    <#if field.type == 'Map'>
    @Override
    public Set<String> get${field.name?cap_first}Ids() {
        return newBean.get${field.name?cap_first}Ids();
    }
    @Override
    public ${field.elementType} get${field.name?cap_first}<@collection_item field=field />(String id) {
        return newBean.get${field.name?cap_first}<@collection_item field=field />(id);
    }
    @Override
    public Map<String,${field.elementType}> get${field.name?cap_first}() {
         return newBean.get${field.name?cap_first}();
    }
    <#else>
    @Override
    public <@field_type field=field /> get${field.name?cap_first}() {
        <#if field.persist>
        <#if field.isGeneratedType()>
            return this.${field.name}UpdateResult == null ? previous.get${field.name?cap_first}() : this.${field.name}UpdateResult;
        <#else>
        return updated != null && updated.is${field.name?cap_first}Set() ? updated.get${field.name?cap_first}() : previous.get${field.name?cap_first}();
        </#if>
        <#else>
        return newBean.get${field.name?cap_first}();
        </#if>
    }
    <#if !field.isCollection()>
    @Override
    public boolean is${field.name?cap_first}Set() {
        <#if field.persist>
        return previous.is${field.name?cap_first}Set() || (updated != null && updated.is${field.name?cap_first}Set());
        <#else>
        return newBean.is${field.name?cap_first}Set();
        </#if>
    }
    </#if>
    </#if>
    @Override
    public boolean was${field.name?cap_first}Updated() {
        return previous.is${field.name?cap_first}Modified();
    }
</#list>

    @Override
    public boolean wasUpdated() {
        return <#list type.persistedFields as field>was${field.name?cap_first}Updated() <#sep>|| </#sep></#list>;
    }

<#if isRoot && optimisticLocking>
    @Override
    public Integer getRevision() {
        return updated == null || updated.getRevision() == null ? previous.getRevision() : updated.getRevision();
    }
</#if>

    @Override
    public ${type.name}UpdatesUpdateResult createUpdatesUpdateResult() {
        return new ${type.name}UpdatesUpdateResult(this);
    }

    <#list type.fields as field>
    <#if field.isGeneratedType()>
        @Override
        public ${field.elementType}UpdateResult get${field.name?cap_first}UpdateResult() {
            return this.${field.name}UpdateResult;
        }
    </#if>
    </#list>


}