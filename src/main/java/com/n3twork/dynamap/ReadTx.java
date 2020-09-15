package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 * A DynamoDB Read Transaction - A synchronous operation that atomically retrieves multiple items from one or
 * more tables (but not from indexes) in a single account and Region. A TransactGetItems call can contain up
 * to 25 TransactGetItem objects, each of which contains a Get structure that specifies an item to retrieve
 * from a table in the account and Region. A call to TransactGetItems cannot retrieve items from tables in
 * more than one AWS account or Region. The aggregate size of the items in the transaction cannot exceed
 * 4 MB.</p>
 * <p>
 * DynamoDB rejects the entire TransactGetItems request if any of the following is true:
 * </p>
 * <ul>
 *     <li>A conflicting operation is in the process of updating an item to be read.</li>
 *     <li>There is insufficient provisioned capacity for the transaction to be completed.</li>
 *     <li>There is a user error, such as an invalid data format.</li>
 *     <li>The aggregate size of the items in the transaction cannot exceed 4 MB.</li>
 * </ul>
 *
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html" target="_top">AWS Developer Guide: Transactions</a>
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html" target="_top">JavaDoc: TransactGetItems</a>
 */
public class ReadTx {
    private static final Logger logger = LoggerFactory.getLogger(ReadTx.class);
    private final AmazonDynamoDB amazonDynamoDB;
    private final ReadOpFactory readOpFactory;
    private final List<GetObjectParams> gets = new ArrayList<>();
    private final DynamapBeanLoader dynamapBeanLoader;

    ReadTx(AmazonDynamoDB amazonDynamoDB, ReadOpFactory readOpFactory, DynamapBeanLoader dynamapBeanLoader) {
        if (null == amazonDynamoDB) {
            throw new NullPointerException();
        }
        this.amazonDynamoDB = amazonDynamoDB;
        if (null == readOpFactory) {
            throw new NullPointerException();
        }
        this.readOpFactory = readOpFactory;
        if (null == dynamapBeanLoader) {
            throw new NullPointerException();
        }
        this.dynamapBeanLoader = dynamapBeanLoader;
    }

    public <T extends DynamapRecordBean> void get(GetObjectParams<T> getObjectParams) {
        gets.add(getObjectParams);
    }

    public List<DynamapRecordBean> exec() {
        List<TransactGetItem> actions = gets.stream().map(g -> new TransactGetItem().withGet(readOpFactory.buildGet(g))).collect(Collectors.toList());
        TransactGetItemsRequest tx = new TransactGetItemsRequest()
                .withTransactItems(actions)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        // Execute the transaction and process the result.
        try {
            TransactGetItemsResult txResult = amazonDynamoDB.transactGetItems(tx);
            List<ItemResponse> itemResponses = txResult.getResponses();
            // From AWS docs: An ordered array of up to 25 ItemResponse objects, each of which corresponds to the TransactGetItem object
            // in the same position in the TransactItems array. Each ItemResponse object contains a Map of the name-value pairs that are
            // the projected attributes of the requested item.
            // If a requested item could not be retrieved, the corresponding ItemResponse object is Null, or if the requested item has
            // no projected attributes, the corresponding ItemResponse object is an empty Map.
            // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html
            List<DynamapRecordBean> result = new ArrayList<>(gets.size());
            for (int i = 0; i < itemResponses.size(); i++) {
                GetObjectParams getObjectParams = gets.get(i);
                ItemResponse itemResponse = itemResponses.get(i);
                if (null == itemResponse) {
                    result.set(i, null);
                    continue;
                }
                result.add(dynamapBeanLoader.loadItem(ItemUtils.toItem(itemResponse.getItem()), getObjectParams.getGetObjectRequest().getResultClass()));
            }
            return result;
        } catch (ResourceNotFoundException rnf) {
            logger.error("One of the table involved in the transaction is not found" + rnf.getMessage());
            throw new RuntimeException(rnf);
        } catch (InternalServerErrorException ise) {
            logger.error("Internal Server Error" + ise.getMessage());
            throw new RuntimeException(ise);
        } catch (TransactionCanceledException tce) {
            logger.error("Transaction Canceled" + tce.getMessage());
            throw new RuntimeException(tce);
        }
    }
}
