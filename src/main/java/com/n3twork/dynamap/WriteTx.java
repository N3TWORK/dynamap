package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A DynamoDB Write Transaction - A synchronous write operation that groups up to 25 action requests. These
 * actions can target items in different tables, but not in different AWS accounts or Regions, and no two
 * actions can target the same item. For example, you cannot both ConditionCheck and Update the same item.
 * The aggregate size of the items in the transaction cannot exceed 4 MB.
 * <p>The actions are completed atomically so that either all of them succeed, or all of them fail. They are defined by the following objects:</p>
 * <ul>
 * <li>Put  —   Initiates a PutItem operation to write a new item. This structure specifies the primary key of the item to be written, the name of the table to write it in, an optional condition expression that must be satisfied for the write to succeed, a list of the item's attributes, and a field indicating whether to retrieve the item's attributes if the condition is not met.</li>
 * <li>Update  —   Initiates an UpdateItem operation to update an existing item. This structure specifies the primary key of the item to be updated, the name of the table where it resides, an optional condition expression that must be satisfied for the update to succeed, an expression that defines one or more attributes to be updated, and a field indicating whether to retrieve the item's attributes if the condition is not met.</li>
 * <li>Delete  —   Initiates a DeleteItem operation to delete an existing item. This structure specifies the primary key of the item to be deleted, the name of the table where it resides, an optional condition expression that must be satisfied for the deletion to succeed, and a field indicating whether to retrieve the item's attributes if the condition is not met.</li>
 * <li>ConditionCheck  —   Applies a condition to an item that is not being modified by the transaction. This structure specifies the primary key of the item to be checked, the name of the table where it resides, a condition expression that must be satisfied for the transaction to succeed, and a field indicating whether to retrieve the item's attributes if the condition is not met.</li>
 * </ul>
 * <p>A successful write transaction returns no data.</p>
 * <p>DynamoDB rejects the entire TransactWriteItems request if any of the following is true:</p>
 * <ul>
 * <li>A condition in one of the condition expressions is not met.</li>
 * <li>An ongoing operation is in the process of updating the same item.</li>
 * <li>There is insufficient provisioned capacity for the transaction to be completed.</li>
 * <li>An item size becomes too large (bigger than 400 KB), a local secondary index (LSI) becomes too large, or a similar validation error occurs because of changes made by the transaction.</li>
 * <li>The aggregate size of the items in the transaction exceeds 4 MB.</li>
 * <li>There is a user error, such as an invalid data format.</li>
 * </ul>
 *
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html" target="_top">AWS Developer Guide: Transactions</a>
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html" target="_top">JavaDoc: TransactWriteItems</a>
 */
public class WriteTx {
    private static final Logger logger = LoggerFactory.getLogger(WriteTx.class);

    private final AmazonDynamoDB amazonDynamoDB;
    private final Set<Update> updates = new HashSet<>();
    private final Set<Put> puts = new HashSet<>();
    private final Set<Delete> deletes = new HashSet<>();
    private final Set<ConditionCheck> conditionChecks = new HashSet<>();
    private final WriteOpFactory writeOpFactory;
    private final DynamoItemFactory dynamoItemFactory;

    WriteTx(AmazonDynamoDB amazonDynamoDB, WriteOpFactory writeOpFactory, DynamoItemFactory dynamoItemFactory) {
        if (null == amazonDynamoDB) {
            throw new NullPointerException();
        }
        this.amazonDynamoDB = amazonDynamoDB;
        if (null == writeOpFactory) {
            throw new NullPointerException();
        }
        this.writeOpFactory = writeOpFactory;
        if (null == dynamoItemFactory) {
            throw new NullPointerException();
        }
        this.dynamoItemFactory = dynamoItemFactory;
    }

    public <T extends DynamapPersisted<U>, U extends RecordUpdates<T>> void update(UpdateParams<T> u) {
        updates.add(writeOpFactory.buildUpdate(u));
    }

    /**
     * This method is deprecated: use save(SaveParams<T>) instead.
     * @param dynamapRecordBean
     * @param <T>
     */
    @Deprecated
    public <T extends DynamapRecordBean> void save(T dynamapRecordBean) {
        puts.add(writeOpFactory.buildPut(dynamapRecordBean, dynamoItemFactory));
    }

    public <T extends DynamapRecordBean> void save(SaveParams<T> saveParams) {
        puts.add(writeOpFactory.buildPut(saveParams, dynamoItemFactory));
    }

    public void delete(DeleteRequest deleteRequest) {
        deletes.add(writeOpFactory.buildDelete(deleteRequest));
    }

    public <T extends DynamapRecordBean> void condition(WriteConditionCheck<T> writeConditionCheck) {
        conditionChecks.add(writeOpFactory.buildConditionCheck(writeConditionCheck));
    }

    public void exec() {
        Collection<TransactWriteItem> actions = new HashSet<>(puts.size() + updates.size() + deletes.size() + conditionChecks.size());
        puts.stream().map(p -> new TransactWriteItem().withPut(p)).forEach(actions::add);
        updates.stream().map(u -> new TransactWriteItem().withUpdate(u)).forEach(actions::add);
        deletes.stream().map(d -> new TransactWriteItem().withDelete(d)).forEach(actions::add);
        conditionChecks.stream().map(c -> new TransactWriteItem().withConditionCheck(c)).forEach(actions::add);

        TransactWriteItemsRequest tx = new TransactWriteItemsRequest()
                .withTransactItems(actions)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        // Execute the transaction and process the result.
        try {
            TransactWriteItemsResult result = amazonDynamoDB.transactWriteItems(tx);
            logger.info("Transaction Successful: {}", result.getConsumedCapacity());
        } catch (ResourceNotFoundException rnf) {
            logger.error("One of the table involved in the transaction is not found {}", rnf.getMessage());
            throw new RuntimeException(rnf);
        } catch (InternalServerErrorException ise) {
            logger.error("Internal Server Error {}", ise.getMessage());
            throw new RuntimeException(ise);
        } catch (TransactionCanceledException tce) {
            logger.error("Transaction Canceled {}", tce.getMessage());
            throw new RuntimeException(tce);
        }
    }
}
