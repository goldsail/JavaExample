package org.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Batch n requests and handle them once. The batch will flush on timeout automatically.
 * This class is expected to have long-running lifecycle.
 * @param <T> Request type.
 * @param <R> Response type.
 */
public class SequentiallyBufferedBatcher<T, R> implements AutoCloseable {

    private final Function<List<T>, List<Result<R>>> batchedRequestHandler;
    private final Predicate<List<T>> shouldFlush;
    private final Duration batchTimeout;
    private final ExecutorService executorService;

    private final Object lock = new Object();
    private List<ItemFuture<T, R>> buffer = new ArrayList<>();
    private boolean isClosed = false;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private record ItemFuture<T, R>(T item, CompletableFuture<R> future) {}

    /**
     * Individual result in a batch, either success or failure.
     * @param response successful response
     * @param exception if not null, the failure exception.
     * @param <R> Response type.
     */
    public record Result<R>(R response, Exception exception) {}

    /**
     * Constructor.
     * @param batchedRequestHandler handles a batch of requests and returns individual successful or failed results;
     *                              the returned list must match the size and ordering of the request batch;
     *                              this class will never call this handler with an empty request list
     * @param shouldFlush decides if a batch is "full" and eligible to flush immediately
     * @param batchTimeout the batch will flush after this timeout even if shouldFlush would return false
     * @param executorService executor service for async task execution; this class does not own its lifecycle;
     *                        you may provide a thread pool or virtual thread per task executor for background execution;
     *                        you may provide a direct executor service if you want to back pressure {@link #handleAsync}
     */
    public SequentiallyBufferedBatcher(final Function<List<T>, List<Result<R>>> batchedRequestHandler,
                                       final Predicate<List<T>> shouldFlush,
                                       final Duration batchTimeout,
                                       final ExecutorService executorService) {
        this.batchedRequestHandler = batchedRequestHandler;
        this.shouldFlush = shouldFlush;
        this.batchTimeout = batchTimeout;
        this.executorService = executorService;
    }

    /**
     * Handles a request asynchronously in batches. Returns a future that will:
     * <ul>
     *     <li><b>complete</b> with <i>the</i> response if {@link #batchedRequestHandler} returns a list of individual results where this request had a response;</li>
     *     <li><b>complete exceptionally</b> with <i>the</i> exception if {@link #batchedRequestHandler} returns a list of individual results in which this request had an exception;</li>
     *     <li><b>complete exceptionally</b> with <i>the</i> exception if {@link #batchedRequestHandler} throws an exception.</li>
     * </ul>
     * @param request request object
     * @return future of the response object
     */
    public CompletableFuture<R> handleAsync(final T request) {
        synchronized (lock) {
            if (isClosed) {
                return CompletableFuture.failedFuture(new IllegalStateException("AsyncBatcher is already closed"));
            }
            final ItemFuture<T, R> itemFuture = new ItemFuture<>(request, new CompletableFuture<>());
            addToBatchWithTimeout(itemFuture);
            final List<T> batch = buffer.stream().map(ItemFuture::item).toList();
            if (shouldFlush.test(batch)) {
                flushBatchImmediatelyAndResetBuffer();
            }
            return itemFuture.future();
        }
    }

    /**
     * Closes the batcher and submits all remaining requests in the buffer to {@link #executorService}.
     * However, it does not wait for {@link #executorService} to finish those requests before closing itself.
     * It does not shut down {@link #executorService} because it is managed externally.
     */
    @Override
    public void close() {
        synchronized (lock) {
            isClosed = true;
            scheduledExecutorService.shutdownNow();
            flushBatchImmediatelyAndResetBuffer();
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
            scheduledExecutorService.schedule(() -> {
                executorService.submit(() -> {
                    synchronized (lock) {
                        final List<ItemFuture<T, R>> batchToFlush = List.copyOf(bufferToAddTimeout);
                        bufferToAddTimeout.clear();
                        handleAndReportFutures(batchToFlush);
                    }
                });
            }, batchTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleAndReportFutures(final List<ItemFuture<T, R>> batchToFlush) {
        if (batchToFlush.isEmpty()) {
            // No operation on empty batch. The batch has already been flushed and reset. Two possibilities:
            // 1. In an earlier request, shouldFlush.test(batch) returned true and triggered an immediate flush;
            // 2. (less common) The batch happens to be empty when close() method is invoked.
            return;
        }
        //
        final int batchSize = batchToFlush.size();
        try {
            final List<T> batchedRequests = batchToFlush.stream().map(ItemFuture::item).toList();
            final List<Result<R>> batchedResults = batchedRequestHandler.apply(batchedRequests);
            if (batchedResults.size() != batchSize) {
                throw new IllegalArgumentException("batchedRequestHandler size mismatch");
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
