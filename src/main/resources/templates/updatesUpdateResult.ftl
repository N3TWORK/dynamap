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

public class ${type.name}UpdatesUpdateResult extends ${type.name}Updates implements ${type.name}UpdateResult {

private ${type.name}UpdateResult updateResult;

public ${type.name}UpdatesUpdateResult(${type.name}UpdateResult updateResult) {
    super(updateResult.createUpdates());
    this.updateResult = updateResult;
}

<#list type.fields as field>
    public boolean was${field.name?cap_first}Updated() {
        return updateResult.was${field.name?cap_first}Updated() || this.is${field.name?cap_first}Modified();
    }
     <#if field.isGeneratedType()>
        @Override
        public ${field.elementType}UpdateResult get${field.name?cap_first}UpdateResult() {
            return updateResult.get${field.name?cap_first}UpdateResult();
        }
     </#if>
</#list>

  @Override
  public boolean wasUpdated() {
    return <#list type.persistedFields as field>updateResult.was${field.name?cap_first}Updated() || this.is${field.name?cap_first}Modified() <#sep>|| </#sep></#list>;
  }

  public ${type.name}UpdatesUpdateResult createUpdatesUpdateResult() {
    return new ${type.name}UpdatesUpdateResult(updateResult);
  }

}