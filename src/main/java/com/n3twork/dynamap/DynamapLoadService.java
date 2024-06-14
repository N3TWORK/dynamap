package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.model.Field;
import com.n3twork.dynamap.model.TableDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * All the logic for loading a raw DynamoDB Item including:
 * <ul>
 *     <li>Any necessary migrations (including writing back to DynamoDB if requested)</li>
 *     <li>Conversion to the appripriate Dynamap Bean type</li>
 * </ul>
 */
class DynamapLoadService {
    private static final Logger logger = LoggerFactory.getLogger(DynamapLoadService.class);
    private final SchemaRegistry schemaRegistry;
    private final DynamapBeanFactory dynamapBeanFactory;
    private final ObjectMapper objectMapper;
    private final String tableNamePrefix;
    private boolean skipMigration = false;
    private boolean writeBack = true;
    private String suffix;
    private DynamoRateLimiter writeRateLimiter;
    private Object migrationContext;
    private TableCache tableCache;

    public DynamapLoadService(SchemaRegistry schemaRegistry, DynamapBeanFactory dynamapBeanFactory, ObjectMapper objectMapper, String tableNamePrefix, TableCache tableCache) {
        if (null == schemaRegistry) {
            throw new IllegalArgumentException();
        }
        this.schemaRegistry = schemaRegistry;
        if (null == dynamapBeanFactory) {
            throw new IllegalArgumentException();
        }
        this.dynamapBeanFactory = dynamapBeanFactory;
        if (null == objectMapper) {
            throw new IllegalArgumentException();
        }
        this.objectMapper = objectMapper;
        if (null == objectMapper) {
            throw new IllegalArgumentException();
        }
        this.tableNamePrefix = tableNamePrefix;
        if (null == tableCache) {
            throw new IllegalArgumentException();
        }
        this.tableCache = tableCache;
    }

    public DynamapLoadService withWriteLimiter(DynamoRateLimiter dynamoRateLimiter) {
        this.writeRateLimiter = dynamoRateLimiter;
        return this;
    }

    public DynamapLoadService writeBack(boolean writeBack) {
        this.writeBack = writeBack;
        return this;
    }

    public DynamapLoadService withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }

    public DynamapLoadService skipMigration(boolean skipMigration) {
        this.skipMigration = skipMigration;
        return this;
    }

    public DynamapLoadService withMigrationContext(Object migrationContext) {
        this.migrationContext = migrationContext;
        return this;
    }

    /**
     * Take a raw DynamoDB item, migrate as needed, convert to a Dynamap bean.
     */
    public <T extends DynamapRecordBean> T loadItem(Map<String, AttributeValue> item, Class<T> resultClass) {
        if (null == item || item.isEmpty()) {
            return null;
        }
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(resultClass);
        if (skipMigration) {
            return dynamapBeanFactory.asDynamapBean(item, resultClass);
        } else {
            MigrationResult migrationResult = migrateItem(item, resultClass, this.migrationContext);
            T result = dynamapBeanFactory.asDynamapBean(migrationResult.item, resultClass);
            if (migrationResult.wasMigrated && writeBack) {
                Item savedItem = new DynamapSaveService(objectMapper, tableNamePrefix, tableCache)
                        .saveBean(result, tableDefinition, true, false, true, writeRateLimiter, suffix, null, null, null);
                // if optimistic locking is enabled then the revision number will have been incremented
                // repopulate the result bean
                if (tableDefinition.isOptimisticLocking()) {
                    result = dynamapBeanFactory.asDynamapBean(savedItem, resultClass);
                }
            }
            return result;
        }
    }

    private static class MigrationResult {
        private final boolean wasMigrated;
        private final Map<String, AttributeValue> item;

        MigrationResult(boolean wasMigrated, Map<String,AttributeValue> item) {
            this.wasMigrated = wasMigrated;
            this.item = item;
        }
    }

    /**
     * Migrate an Item if necessary.
     */
    private <T extends DynamapRecordBean> MigrationResult migrateItem(Map<String, AttributeValue> item, Class<T> resultClass, Object migrationContext) {
        if (null == item || item.isEmpty()) {
            return new MigrationResult(false, null);
        }

        TableDefinition tableDefinition = this.schemaRegistry.getTableDefinition(resultClass);
        if (!tableDefinition.isEnableMigrations()) {
            return new MigrationResult(false, item);
        }

        String schemaField = tableDefinition.getSchemaVersionField();
        int currentVersion = 0;
        if (!item.containsKey(schemaField)) {
            Field field = tableDefinition.getField(tableDefinition.getHashKey());
            logger.warn("Schema version field does not exist for {} on item with hash key {}. Migrating item to current version", tableDefinition.getTableName(), item.get(field.getDynamoName()));
        } else {
            currentVersion = Integer.parseInt(schemaField);
        }

        if (currentVersion > tableDefinition.getVersion()) {
            throw new UnsupportedSchemaVersionException("Document schema has been migrated to a version later than this release supports: Document version: " + currentVersion + ", Supported version: " + tableDefinition.getVersion());
        }

        if (currentVersion < tableDefinition.getVersion()) {
            List<Migration> migrations = schemaRegistry.getMigrations(resultClass);
            if (migrations != null) {
                for (Migration migration : migrations) {
                    if (migration.getVersion() > currentVersion) {
                        migration.migrate(item, currentVersion, migrationContext);
                    }
                }
                for (Migration migration : migrations) {
                    if (migration.getVersion() > currentVersion) {
                        migration.postMigration(item, currentVersion, migrationContext);
                    }
                }
            }

            item.put(schemaField, AttributeValue.builder().n(String.valueOf(tableDefinition.getVersion())).build());
            return new MigrationResult(true, item);
        }
        return new MigrationResult(false, item);
    }
}
