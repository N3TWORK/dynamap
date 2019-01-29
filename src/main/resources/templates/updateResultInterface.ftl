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

public interface ${type.name}UpdateResult extends ${type.name}, UpdateResult<${type.name}, ${type.name}Updates> {

<#list type.fields as field>
    public boolean was${field.name?cap_first}Updated();
    <#if field.isGeneratedType()>
    ${field.elementType}UpdateResult get${field.name?cap_first}UpdateResult();
    </#if>
</#list>

    public ${type.name}UpdatesUpdateResult createUpdatesUpdateResult();

}