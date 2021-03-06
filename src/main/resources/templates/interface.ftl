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

import java.util.*;
import com.n3twork.dynamap.*;
<#list imports as import>
import ${import};
</#list>

public interface ${type.name} extends DynamapPersisted<${updatesName}> {

<#if isRoot>
    public static final Integer SCHEMA_VERSION = ${schemaVersion};
    String SCHEMA_VERSION_FIELD = "${schemaVersionField}";
</#if>
<#if isRoot && optimisticLocking>
    String REVISION_FIELD = "${revisionFieldName}";
</#if>
<#list type.fields as field>
    <#if field.isPersist()>
    String ${field.name?upper_case}_FIELD = "${field.dynamoName}";
    <#elseif field.isSerialize()>
    String ${field.name?upper_case}_FIELD = "${field.name}";
    </#if>
</#list>

<#if tableDefinition.isEnableMigrations() && isRoot>
    Integer getDynamapSchemaVersion();
</#if>
<#list type.fields as field>
    <#if field.type == 'Map'>
    Set<String> get${field.name?cap_first}Ids();
    ${field.elementType} get${field.name?cap_first}<@collection_item field=field />(String id);
    Map<String,${field.elementType}> get${field.name?cap_first}();
    <#else>
    <@field_type field=field /> get${field.name?cap_first}();
    <#if !field.isCollection()>
    boolean is${field.name?cap_first}Set();
    </#if>
    </#if>
</#list>
<#if isRoot && optimisticLocking>
    Integer getRevision();
</#if>
}