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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TableDefinition {

    private final String tableName;
    private final String packageName;
    private final String type;
    private final String hashKey;
    private final String rangeKey;
    private final int version;
    private final List<Type> types;
    private final List<Index> globalSecondaryIndexes;

    @JsonCreator
    public TableDefinition(@JsonProperty("table") String tableName, @JsonProperty("package") String packageName, @JsonProperty("type") String type, @JsonProperty("hashkey") String hashKey, @JsonProperty("rangekey") String rangeKey,
                           @JsonProperty("version") int version, @JsonProperty("fields") List<Field> fields, @JsonProperty("types") List<Type> types, @JsonProperty("globalSecondaryIndexes") List<Index> globalSecondaryIndexes) {
        this.tableName = tableName;
        this.packageName = packageName;
        this.type = type;
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
        this.version = version;
        this.types = types;
        this.globalSecondaryIndexes = globalSecondaryIndexes;
    }

    public String getTableName() {
        return tableName;
    }

    @JsonIgnore
    public String getTableName(String prefix) {
        if (prefix != null) {
            return prefix + "." + tableName;
        }
        return tableName;
    }


    public String getPackageName() {
        return packageName;
    }

    public String getType() {
        return type;
    }

    public String getHashKey() {
        return hashKey;
    }

    public String getRangeKey() {
        return rangeKey;
    }

    public int getVersion() {
        return version;
    }

    public List<Type> getTypes() {
        return types;
    }

    public List<Index> getGlobalSecondaryIndexes() {
        return globalSecondaryIndexes;
    }

    public Field getField(String fieldName) {
        Type tableType = getTypes().stream().filter(t -> t.getName().equals(getType())).findFirst().get();
        return tableType.getFields().stream().filter(f -> f.getName().equals(fieldName)).findFirst().get();
    }
}
