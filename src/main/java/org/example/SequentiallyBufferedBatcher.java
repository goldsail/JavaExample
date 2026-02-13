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
    protected final CompletableFuture<R> handleAsync(final T request) {
        synchronized (lock) {
            if (isClosed) {
                throw new IllegalStateException("AsyncBatcher is already closed");
            }
            final ItemFuture<T, R> itemFuture = new ItemFuture<>(request, new CompletableFuture<>());
            addToBatchWithTimeout(itemFuture);
            final List<T> requestsInBuffer = buffer.stream().map(ItemFuture::item).toList();
            if (shouldFlush(requestsInBuffer)) {
                flushBatchImmediatelyAndResetBuffer();
            }
            return itemFuture.future();
        }
    }

    /**
     * Closes the batcher and submits all remaining requests in the buffer to {@link #handleBatch}.
     * It waits for all buffered requests to finish before closing itself;
     * however, if {@link #provideBatchTimeout} times out, it will print a warning message and return anyway.
     */
    @Override
    public final void close() {
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
     * @return a batch of results (either success or failure) of the same size
     */
    protected abstract List<Result<R>> handleBatch(List<T> requests);

    /**
     * Decides if a batch is full and eligible to flush immediately
     * @param requests all buffered requests
     * @return true if we should flush the batch
     */
    protected abstract boolean shouldFlush(List<T> requests);

    /**
     * The batch will flush after this timeout in case of inactivity
     * @return batch timeout
     */
    protected abstract Duration provideBatchTimeout();

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
        buffer.clear();
        buffer = new ArrayList<>();
        executorService.submit(() -> handleAndReportFutures(batchToFlush));
    }

    private void addToBatchWithTimeout(final ItemFuture<T, R> itemFuture) {
        buffer.add(itemFuture);
        if (buffer.size() == 1) {
            // No need to schedule timeout for every buffer item; just need the first and earliest one.
            final List<ItemFuture<T, R>> bufferToAddTimeout = buffer;
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
                    handleAndReportFutures(batchToFlush);
                }
            });
        }
    }

    private void handleAndReportFutures(final List<ItemFuture<T, R>> batchToFlush) {
        if (batchToFlush.isEmpty()) {
            // No operation on empty batch. The batch has already been flushed and reset. Reason:
            // The batch happens to be empty when close() method is invoked.
            return;
        }
        //
        final int batchSize = batchToFlush.size();
        try {
            final List<T> batchedRequests = batchToFlush.stream().map(ItemFuture::item).toList();
            final List<Result<R>> batchedResults = handleBatch(batchedRequests);
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
