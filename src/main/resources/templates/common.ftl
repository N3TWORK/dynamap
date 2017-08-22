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

<#macro field_type field>
    <#compress>
        <#if field.multiValue??>
            <#if field.multiValue! == 'MAP'>
            Map<String,${field.type}>
            <#elseif field.multiValue! == 'LIST'>
            List<${field.type}>
            <#elseif field.multiValue! == 'SET'>
            Set<${field.type}>
            </#if>
        <#else>
        ${field.type}
        </#if>
    </#compress>
</#macro>

<#macro collection_item field>
    <#compress>
        <#if field.isNumber()>
        Value
        <#else>
        Item
        </#if>
    </#compress>
</#macro>

<#macro defaultNumber field>
    <#compress>
        <#if field.defaultValue == 'null'>
            <#if field.type == 'Integer'>
            0
            <#elseif field.type == 'Long'>
            0L
            <#elseif field.type == 'Float'>
            0.0
            <#elseif field.type == 'Double'>
            0.0
            </#if>
        <#else>
        ${field.defaultValue}
        </#if>
    </#compress>
</#macro>

<#macro persisted_modified field>
    <#compress>
        <#if !field.isPersist()>
            persistedModified = true;
        </#if>
    </#compress>
</#macro>