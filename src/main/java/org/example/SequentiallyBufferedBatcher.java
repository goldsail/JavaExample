package org.example;

import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Batch n requests and handle them once. The batch will flush on timeout automatically.
 * This class is expected to have long-running lifecycle.
 * @param <T> Request type.
 * @param <R> Response type.
 */
@ThreadSafe
@NoArgsConstructor
public abstract class SequentiallyBufferedBatcher<T, R> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequentiallyBufferedBatcher.class);

    private final Object lock = new Object();
    @GuardedBy("lock") private List<ItemFuture<T, R>> buffer = new ArrayList<>();
    @GuardedBy("lock") private String batchName = generateBatchName();
    @GuardedBy("lock") private long bufferOffset = 0;
    @GuardedBy("lock") private boolean isClosed = false;

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    private record ItemFuture<T, R>(T item, CompletableFuture<R> future) {}

    /**
     * Individual result in a batch, either success or failure.
     * @param response successful response
     * @param exception if not null, the failure exception.
     * @param <R> Response type.
     */
    public record Result<R>(R response, Exception exception) {}

    /**
     * Pointer to location in the batch.
     * @param batchName name of the batch.
     * @param startOffset start offset, inclusive
     * @param endOffset end offset, exclusive
     */
    public record BatchLocation(String batchName, long startOffset, long endOffset) {}

    /**
     * An entry in the batch request. Contains a location pointer and the response future.
     * @param location batch location
     * @param response response future
     * @param <R> Response type.
     */
    public record BatchEntry<R>(BatchLocation location, CompletableFuture<R> response) {}

    /**
     * Handles a request asynchronously in batches. Returns a future that will:
     * <ul>
     *     <li><b>complete</b> with <i>the</i> response if {@link #handleBatch} returns a list of individual results where this request had a response;</li>
     *     <li><b>complete exceptionally</b> with <i>the</i> exception if {@link #handleBatch} returns a list of individual results in which this request had an exception;</li>
     *     <li><b>complete exceptionally</b> with <i>the</i> exception if {@link #handleBatch} throws an exception.</li>
     * </ul>
     * Also returns the batch location immediately, since they are precomputed.
     * @param request request object
     * @return future of the response object
     */
    public BatchEntry<R> handleAsync(final T request) {
        synchronized (lock) {
            if (isClosed) {
                throw new IllegalStateException("AsyncBatcher is already closed");
            }
            final ItemFuture<T, R> itemFuture = new ItemFuture<>(request, new CompletableFuture<>());
            final BatchLocation location = addToBatchWithTimeout(itemFuture);
            if (shouldFlush(bufferOffset)) {
                flushBatchImmediatelyAndResetBuffer();
            }
            return new BatchEntry<>(location, itemFuture.future());
        }
    }

    /**
     * Closes the batcher and submits all remaining requests in the buffer to {@link #executorService}.
     * It waits for {@link #executorService} to finish those requests before closing itself;
     * however, if {@link #provideBatchTimeout} times out, it will print a warning message and return anyway.
     */
    @Override
    public void close() {
        synchronized (lock) {
            isClosed = true;
            flushBatchImmediatelyAndResetBuffer();
            executorService.shutdown();
            try {
                final long timeout = provideBatchTimeout().toMillis();
                if (!executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
                    LOGGER.warn("Executor is not clean after {} ms timeout!", timeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Handles a batch of requests and returns individual successful or failed results;
     * the returned list must match the size and ordering of the request batch;
     * this class will never call this handler with an empty request list.
     * @param requests a batch of requests
     * @param batchName batch name
     * @return a batch of results (either success or failure) of the same size
     */
    protected abstract List<Result<R>> handleBatch(List<T> requests, String batchName);

    /**
     * Measure the logical size of a request.
     * <ul>
     *     <li><b>Example 1:</b> Always return 1. The current batch offset equals how many requests in the batch.</li>
     *     <li><b>Example 2:</b> Return binary array size. The current batch offset points to binary data offset.</li>
     * </ul>
     * @param request request
     * @return logical size, must be greater than or equal to 0
     */
    protected abstract long measureSize(T request);

    /**
     * Decides if a batch is full and eligible to flush immediately
     * @param currentBatchOffset current batch size, i.e. total size of all buffered requests
     * @return true if we should flush the batch
     */
    protected abstract boolean shouldFlush(long currentBatchOffset);

    /**
     * The batch will flush after this timeout in case of inactivity
     * @return batch timeout
     */
    protected abstract Duration provideBatchTimeout();

    /**
     * Generate a new batch name. Example: UUID strings
     * @return new batch name
     */
    protected abstract String generateBatchName();

    protected static void sleepWithInterruption(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            LOGGER.warn("Interrupted while sleeping.", e);
            Thread.currentThread().interrupt();
        }
    }

    private void flushBatchImmediatelyAndResetBuffer() {
        final List<ItemFuture<T, R>> batchToFlush = List.copyOf(buffer);
        final String batchNameToFlush = batchName;
        buffer.clear();
        buffer = new ArrayList<>();
        bufferOffset = 0;
        batchName = generateBatchName();
        executorService.submit(() -> handleAndReportFutures(batchToFlush, batchNameToFlush));
    }

    private BatchLocation addToBatchWithTimeout(final ItemFuture<T, R> itemFuture) {
        final long startOffset = bufferOffset;
        buffer.add(itemFuture);
        final long requestSize = measureSize(itemFuture.item);
        if (requestSize < 0) {
            throw new IllegalArgumentException("measureSize cannot return negative numbers");
        }
        bufferOffset += requestSize;
        final long endOffset = bufferOffset;
        final BatchLocation location = new BatchLocation(batchName, startOffset, endOffset);
        if (buffer.size() == 1) {
            // No need to schedule timeout for every buffer item; just need the first and earliest one.
            final List<ItemFuture<T, R>> bufferToAddTimeout = buffer;
            final String batchNameToAddTimeout = batchName;
            executorService.submit(() -> {
                sleepWithInterruption(provideBatchTimeout().toMillis());
                synchronized (lock) {
                    if (bufferToAddTimeout.isEmpty()) {
                        // No operation on empty batch. The batch has already been flushed and reset. Reason:
                        // In an earlier request, shouldFlush() returned true and triggered an immediate flush;
                        return;
                    }
                    final List<ItemFuture<T, R>> batchToFlush = List.copyOf(bufferToAddTimeout);
                    bufferToAddTimeout.clear();
                    bufferOffset = 0;
                    batchName = generateBatchName();
                    handleAndReportFutures(batchToFlush, batchNameToAddTimeout);
                }
            });
        }
        return location;
    }

    private void handleAndReportFutures(final List<ItemFuture<T, R>> batchToFlush, final String batchNameToFlush) {
        if (batchToFlush.isEmpty()) {
            // No operation on empty batch. The batch has already been flushed and reset. Reason:
            // The batch happens to be empty when close() method is invoked.
            return;
        }
        //
        final int batchSize = batchToFlush.size();
        try {
            final List<T> batchedRequests = batchToFlush.stream().map(ItemFuture::item).toList();
            final List<Result<R>> batchedResults = handleBatch(batchedRequests, batchNameToFlush);
            if (batchedResults.size() != batchSize) {
                throw new IllegalArgumentException("handleBatch size mismatch");
            }
            for (int i = 0; i < batchSize; i++) {
                final Result<R> result = batchedResults.get(i);
                if (result.exception() != null) {
                    batchToFlush.get(i).future().completeExceptionally(result.exception());
                } else {
                    batchToFlush.get(i).future().complete(result.response());
                }
            }
        } catch (Throwable throwable) {
            for (int i = 0; i < batchSize; i++) {
                batchToFlush.get(i).future().completeExceptionally(throwable);
            }
        }
    }
}
