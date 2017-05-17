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
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaRegistry {

    private final Schema schema;

    private final Map<String, List<Migration>> tableMigrations = new HashMap<>();
    private final Map<String, Method> getTableNameMethods = new HashMap<>();


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
        migrations.sort((m1, m2) -> m1.getVersion() == m2.getVersion() ? Integer.compare(m1.getSequence(), m2.getSequence()) : Integer.compare(m1.getVersion(), m2.getVersion()));
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

    public <T extends DynamapRecordBean> TableDefinition getTableDefinition(Class<T> clazz) {
        String tableName;
        try {
            Method getMethod = getTableNameMethods.get(clazz.getCanonicalName());
            if (getMethod == null) {
                Class bean = clazz;
                while (!bean.getSuperclass().getName().equals("java.lang.Object")) {
                    bean = bean.getSuperclass();
                }
                getMethod = bean.getMethod("getTableName");
                getTableNameMethods.put(clazz.getCanonicalName(), getMethod);
            }
            tableName = (String) getMethod.invoke(clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getTableDefinition(tableName);
    }


}
