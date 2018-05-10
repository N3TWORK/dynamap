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
        <#if field.isCollection()>
            <#if field.type == 'Map'>
            Map<String,${field.elementType}>
            <#elseif field.type == 'List'>
            List<${field.elementType}>
            <#elseif field.type == 'Set'>
            Set<${field.elementType}>
            </#if>
        <#else>
        ${field.elementType}
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

<#macro defaultValue field elementOnly>
    <#compress>
      <#if field.defaultValue??>
          <#if field.isCollection() && !elementOnly>
            <@default_collection field=field />
          <#else>
              <#if field.elementType == 'String'>
              "${field.defaultValue}"
              <#else>
              ${field.defaultValue}
              </#if>
          </#if>
     <#else>
         <#if field.isNumber() && elementOnly>
            <@numberSuffix field 0 />
         <#elseif field.isCollection()>
            <@default_collection field=field />
         <#else>
            null
         </#if>
     </#if>
    </#compress>
</#macro>

<#macro numberSuffix field value>
<#compress>
    <#if field.elementType == 'Long'>
    ${value}L
    <#elseif field.elementType = 'Float'>
    ${value}f
    <#elseif field.elementType = 'Double'>
    ${value}d
    <#else>
    ${value}
    </#if>
</#compress>
</#macro>

<#macro default_collection field>
<#compress>
    <#if field.type == 'Map'>
        Collections.emptyMap()
    <#elseif field.type == 'Set'>
        Collections.emptySet()
    <#else>
        Collections.emptyList()
    </#if>
</#compress>
</#macro>

<#macro persisted_modified field>
        <#if field.isPersist()>
        persistedModified = true;
        </#if>
</#macro>