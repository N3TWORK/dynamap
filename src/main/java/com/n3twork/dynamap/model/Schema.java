/*
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
*/


package com.n3twork.dynamap.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;

public class Schema {

    public static final String REVISION_FIELD = "_rv";

    private final Map<String, TableDefinition> tableDefinitionsByName = new HashMap<>();

    public Schema(@JsonProperty("tables") List<TableDefinition> tableDefinitions) {
        for (TableDefinition tableDefinition : tableDefinitions) {
            tableDefinitionsByName.put(tableDefinition.getTableName(), tableDefinition);
            Set<String> generatedTypeNames = tableDefinition.getTypes().stream().map(Type::getName).collect(Collectors.toSet());
            for (Type type : tableDefinition.getTypes()) {
                for (Field field : type.getFields()) {
                    if (generatedTypeNames.contains(field.getType())) {
                        field.setGeneratedType(true);
                    }
                }
            }
        }
    }

    public Collection<TableDefinition> getTableDefinitions() {
        return tableDefinitionsByName.values();
    }
}
