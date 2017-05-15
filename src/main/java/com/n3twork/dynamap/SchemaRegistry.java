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
import com.n3twork.dynamap.model.Field;
import com.n3twork.dynamap.model.Schema;
import com.n3twork.dynamap.model.TableDefinition;
import com.n3twork.dynamap.model.Type;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SchemaRegistry {

    private final Schema schema;

    private Map<String, List<Migration>> tableMigrations;


    public SchemaRegistry(InputStream schemaInput) {
        try {
            this.schema = new ObjectMapper().readValue(schemaInput, Schema.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(schemaInput);
        }
    }

    public void registerMigration(String tableName, Migration migration) {

        List<Migration> migrations = tableMigrations.get(tableName);
        if (migrations == null) {
            migrations = new ArrayList<>();
            tableMigrations.put(tableName, migrations);
        }
        migrations.add(migration);
        migrations.sort(Comparator.comparingInt(m -> m.getVersion()));
    }

    public List<Migration> getMigrations(String tableName) {
        return tableMigrations.get(tableName);
    }

    public Schema getSchema() {
        return schema;
    }

    public TableDefinition getTableDefinition(String tableName) {
        return schema.getTableDefinitions().stream().filter(t -> t.getTableName().equals(tableName)).findFirst().get();
    }

    public <T extends DynaMapPersisted> TableDefinition getTableDefinition(Class<T> clazz) {
        //todo : cache method
        String tableName;
        try {
            tableName = (String) clazz.getMethod("getTableName").invoke(clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getTableDefinition(tableName);
    }


}
