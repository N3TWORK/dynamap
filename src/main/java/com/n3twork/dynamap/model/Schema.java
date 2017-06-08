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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Schema {

    public static final String SCHEMA_VERSION_FIELD = "_schv";
    public static final String REVISION_FIELD = "_rv";

    private final List<TableDefinition> tableDefinitions;
    private final Map<String, String> tablesForClass = new HashMap<>();

    public Schema(@JsonProperty("registryClass") String registryClass, @JsonProperty("tables") List<TableDefinition> tableDefinitions) {

        this.tableDefinitions = tableDefinitions;

        for (TableDefinition tableDefinition : tableDefinitions) {
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

    public List<TableDefinition> getTableDefinitions() {
        return tableDefinitions;
    }
}
