package org.example;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Batches up to {@value #MAX_BATCH_SIZE} PutItem requests and call DynamoDB BatchWriteItem API once.
 * Returns a {@see #CompletableFuture} indicating the individual results (success or failure).
 * It has built-in timeout (if buffer is idle) and at-most-once backoff retry on the DynamoDB call.
 * @param <T> DynamoDb bean for table item
 */
@RequiredArgsConstructor
public class DynamoDbBatchWriter<T> extends SequentiallyBufferedBatcher<T, Void> {

    private static final int MAX_BATCH_SIZE = 25;
    private static final Duration TIMEOUT_TO_FLUSH = Duration.ofMillis(30);
    private static final int MAX_ATTEMPTS = 2;
    private static final int RETRY_DELAY_MIN = 50;
    private static final int RETRY_DELAY_MAX = 100;

    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
    private final Class<T> itemClass;
    private final DynamoDbTable<T> table;

    @Override
    protected List<Result<Void>> handleBatch(final List<T> requests, final String batchName) {

        List<T> unprocessedItems = requests;
        int attempt = 0;
        while (attempt < MAX_ATTEMPTS && !unprocessedItems.isEmpty()) {
            if (attempt > 0) {
                sleepWithInterruption(ThreadLocalRandom.current().nextInt(RETRY_DELAY_MIN, RETRY_DELAY_MAX));
            }
            attempt++;
            //
            final WriteBatch.Builder<T> writeBatch = WriteBatch.builder(itemClass).mappedTableResource(table);
            unprocessedItems.stream().forEach(writeBatch::addPutItem);
            final BatchWriteItemEnhancedRequest request = BatchWriteItemEnhancedRequest.builder()
                    .writeBatches(writeBatch.build())
                    .build();
            final BatchWriteResult response = dynamoDbEnhancedClient.batchWriteItem(request);
            unprocessedItems = response.unprocessedPutItemsForTable(table);
        }

        final Set<T> unprocessedItemsAfterMaxRetry = Set.copyOf(unprocessedItems);
        return requests.stream().map(request -> {
            if (unprocessedItemsAfterMaxRetry.contains(request)) {
                final RuntimeException exception = new RuntimeException("Failed to write request " + request);
                return new Result<Void>(null, exception);
            } else {
                return new Result<Void>(null, null);
            }
        }).toList();
    }

    @Override
    protected long measureSize(final T request) {
        return 1;
    }

    @Override
    protected boolean shouldFlush(final long currentBatchOffset) {
        return currentBatchOffset >= MAX_BATCH_SIZE;
    }

    @Override
    protected Duration provideBatchTimeout() {
        return TIMEOUT_TO_FLUSH;
    }

    @Override
    protected String generateBatchName() {
        return "not used";
    }
}
