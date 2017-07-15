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

package com.n3twork.dynamap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.model.Schema;
import com.n3twork.dynamap.model.TableDefinition;
import com.n3twork.dynamap.model.Type;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.*;

public class SchemaRegistry {

    private final Schema schema;

    private final Map<Class<? extends DynamapRecordBean>, List<Migration>> tableMigrations = new HashMap<>();
    private final Map<String, TableDefinition> classToTableDefinitions = new HashMap<>();

    public SchemaRegistry(InputStream... schemaInput) {
        List<TableDefinition> tableDefinitions = new ArrayList<>();
        for (InputStream inputStream : schemaInput) {
            try {
                Schema schema = new ObjectMapper().readValue(inputStream, Schema.class);
                tableDefinitions.addAll(schema.getTableDefinitions());
                buildTableDefinitionNames(tableDefinitions);

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
        }
        this.schema = new Schema(tableDefinitions);
    }

    public void registerMigration(Class<? extends DynamapRecordBean> resultClass, Migration migration) {

        if (!getTableDefinition(resultClass).isEnableMigrations()) {
            throw new RuntimeException("Migrations have not been enabled for " + resultClass.getCanonicalName());
        }
        List<Migration> migrations = tableMigrations.get(resultClass);
        if (migrations == null) {
            migrations = new ArrayList<>();
            tableMigrations.put(resultClass, migrations);
        }
        migrations.add(migration);
        migrations.sort(Comparator.comparingInt(m -> m.getVersion()));
    }

    public List<Migration> getMigrations(Class<? extends DynamapRecordBean> resultClass) {
        return tableMigrations.get(resultClass);
    }

    public Schema getSchema() {
        return schema;
    }

    public TableDefinition getTableDefinition(String tableName) {
        return schema.getTableDefinition(tableName);
    }


    public <T extends DynamapRecordBean> TableDefinition getTableDefinition(Class<T> clazz) {
        if (clazz.getCanonicalName().equals("java.lang.Object")) {
            return null;
        }
        TableDefinition tableDefinition = classToTableDefinitions.get(clazz.getCanonicalName());
        if (tableDefinition == null) {
            tableDefinition = getTableDefinition((Class<T>) clazz.getSuperclass());
            if (tableDefinition != null) {
                classToTableDefinitions.put(clazz.getCanonicalName(), tableDefinition);
            }
        }
        return tableDefinition;
    }

    private void buildTableDefinitionNames(List<TableDefinition> tableDefinitions) {
        for (TableDefinition tableDefinition : tableDefinitions) {
            for (Type type : tableDefinition.getTypes()) {
                classToTableDefinitions.put(tableDefinition.getPackageName() + "." + type.getName(), tableDefinition);
                classToTableDefinitions.put(tableDefinition.getPackageName() + "." + type.getName() + "Bean", tableDefinition);
            }
        }
    }


}
