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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableDefinition {

    private static final String DEFAULT_SCHEMA_VERSION_FIELD = "_schv";

    private final String tableName;
    private final String description;
    private final String packageName;
    private final String type;
    private final String hashKey;
    private final String rangeKey;
    private final int version;
    private final List<Type> types;
    private final List<Index> globalSecondaryIndexes;
    private final List<Index> localSecondaryIndexes;
    private final boolean optimisticLocking;
    private final String schemaVersionField;
    private final boolean enableMigrations;

    @JsonCreator
    public TableDefinition(@JsonProperty("table") String tableName, @JsonProperty("description") String description, @JsonProperty("package") String packageName, @JsonProperty("type") String type, @JsonProperty("hashKey") String hashKey, @JsonProperty("rangeKey") String rangeKey,
                           @JsonProperty("version") int version, @JsonProperty("fields") List<Field> fields, @JsonProperty("types") List<Type> types, @JsonProperty("globalSecondaryIndexes") List<Index> globalSecondaryIndexes, @JsonProperty("localSecondaryIndexes") List<Index> localSecondaryIndexes, @JsonProperty("optimisticLocking") boolean optimisticLocking,
                           @JsonProperty("schemaVersionField") String schemaVersionField, @JsonProperty("enableMigrations") Boolean enableMigrations) {
        this.tableName = tableName;
        this.description = description;
        this.packageName = packageName;
        this.type = type;
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
        this.version = version;
        this.types = types;
        this.globalSecondaryIndexes = globalSecondaryIndexes;
        this.localSecondaryIndexes = localSecondaryIndexes;
        this.optimisticLocking = optimisticLocking;
        this.schemaVersionField = schemaVersionField == null ? DEFAULT_SCHEMA_VERSION_FIELD : schemaVersionField;
        this.enableMigrations = enableMigrations == null ? Boolean.TRUE : enableMigrations;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDescription() {
        return description;
    }

    @JsonIgnore
    public String getTableName(String prefix) {
        return getTableName(prefix, null);
    }

    @JsonIgnore
    public String getTableName(String prefix, String suffix) {
        String fullTableName = "";
        if (prefix != null) {
            fullTableName = prefix + tableName;
        }
        if (suffix != null) {
            fullTableName += suffix;
        }

        return fullTableName;
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

    public List<Index> getLocalSecondaryIndexes() {
        return localSecondaryIndexes;
    }

    public boolean isOptimisticLocking() {
        return optimisticLocking;
    }

    public String getSchemaVersionField() {
        return schemaVersionField;
    }

    public boolean isEnableMigrations() {
        return enableMigrations;
    }

    public Field getField(String fieldName) {
        Type tableType = getTypes().stream().filter(t -> t.getName().equals(getType())).findFirst().get();
        return tableType.getFields().stream().filter(f -> f.getName().equals(fieldName)).findFirst().get();
    }

    public Type getFieldType(String type) {
        return getTypes().stream().filter(t -> t.getName().equals(type)).findFirst().get();

    }
}
