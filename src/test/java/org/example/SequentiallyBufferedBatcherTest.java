package org.example;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SequentiallyBufferedBatcherTest {

    SequentiallyBufferedBatcher<Request, Response> target;

    record Request(int userId) {}

    record Response(String userName) {}

    static final int MAX_BATCH_SIZE = 25;
    static final int BATCH_TIMEOUT_MILLIS = 50;
    static final int MAX_ALLOWED_GC_PAUSE = 500;
    static final int MIN_SIMULATED_LATENCY_MILLIS = 5;
    static final int MAX_SIMULATED_LATENCY_MILLIS = 10;
    static final int UNLUCKY_USER_ID = 12345;

    static class TestClass extends SequentiallyBufferedBatcher<Request, Response> {

        /**
         * Batch API to fetch username from user id. It simulates request latency and transient errors.
         * @param requests a batch of requests (user ids)
         * @return return success or failure for each user id, or
         *         occasionally, for user id {@link #UNLUCKY_USER_ID}, throw exception to fail entire batch
         */
        @Override
        protected List<SequentiallyBufferedBatcher.Result<Response>> handleBatch(final List<Request> requests) {

            // Simulate network request latency with small jitter.
            sleepWithInterruption(ThreadLocalRandom.current().nextInt(MIN_SIMULATED_LATENCY_MILLIS, MAX_SIMULATED_LATENCY_MILLIS));

            if (requests.isEmpty()) {
                fail("SequentiallyBufferedBatcher should never invoke an empty batch");
            }

            if (requests.size() > MAX_BATCH_SIZE) {
                fail("SequentiallyBufferedBatcher should never exceed the batch size limit determined by shouldFlush()");
            }

            if (requests.stream().anyMatch(request -> request.userId == UNLUCKY_USER_ID)) {
                // Simulate occasional exception: The entire batch failed
                throw new RuntimeException("Transient exception");
            }

            return requests.stream().map(request -> {
                final int userId = request.userId;
                // Simulate a partial failure: Most requests succeed, a small amount fail.
                if (userId % 400 == 0) {
                    return new SequentiallyBufferedBatcher.Result<Response>(null, new RuntimeException("User id does not exist: " + userId));
                } else {
                    return new SequentiallyBufferedBatcher.Result<>(new Response("User name for id " + userId), null);
                }
            }).toList();
        }

        /**
         * Decides if the batch is full and ready to flush
         * @param requests a batch of requests (user ids)
         * @return check if the batch size has reached {@link #MAX_BATCH_SIZE}
         */
        @Override
        protected boolean shouldFlush(final List<Request> requests) {
            return requests.size() >= MAX_BATCH_SIZE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Duration provideBatchTimeout() {
            return Duration.ofMillis(BATCH_TIMEOUT_MILLIS);
        }
    }

    @BeforeEach
    void setUp() {
        target = new TestClass();
    }

    @AfterEach
    void tearDown() {
        target.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 24, 25, 26, 99, 100, 101, 675, 4399, 12344, 12345, 12346, 50000, 50021, 310256})
    void test(final int totalRequests) throws InterruptedException {

        final ConcurrentMap<Integer, SequentiallyBufferedBatcher.Result<Response>> results = new ConcurrentHashMap<>();
        try (ExecutorService executorService = Executors.newFixedThreadPool(100)) {

            // 1. Make a lot of individual requests using 100 threads. Record the result.
            IntStream.range(0, totalRequests).forEach(userId -> executorService.submit(() -> {
                final Request request = new Request(userId);
                target.handleAsync(request).whenComplete((response, error) -> {
                    if (error != null) {
                        results.put(request.userId, new SequentiallyBufferedBatcher.Result<>(null, (Exception) error));
                    } else {
                        results.put(request.userId, new SequentiallyBufferedBatcher.Result<>(response, null));
                    }
                });
            }));

            // 2. Test the latency SLA. Latency should not exceed the batch timeout plus the max request latency.
            Thread.sleep(BATCH_TIMEOUT_MILLIS + MAX_SIMULATED_LATENCY_MILLIS + MAX_ALLOWED_GC_PAUSE);
            executorService.shutdown();
            if (!executorService.awaitTermination(MAX_ALLOWED_GC_PAUSE, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
                fail("Potential dead lock detected.");
            }
        }

        // 3. Verify results for individual requests
        final List<Integer> transientlyFailedUserIds = new ArrayList<>();
        IntStream.range(0, totalRequests).forEach(userId -> {
            final SequentiallyBufferedBatcher.Result<Response> actual = results.get(userId);
            assertNotNull(actual, "Buffer is not clean! No response recorded for user id: " + userId);
            if (actual.exception() != null && "Transient exception".equals(actual.exception().getMessage())) {
                assertInstanceOf(RuntimeException.class, actual.exception());
                transientlyFailedUserIds.add(userId);
                return;
            }
            if (userId % 400 == 0) {
                final String expected = "User id does not exist: " + userId;
                assertNotNull(actual.exception());
                assertNull(actual.response());
                assertInstanceOf(RuntimeException.class, actual.exception());
                assertEquals(expected, actual.exception().getMessage());
            } else {
                final String expected = "User name for id " + userId;
                assertNull(actual.exception());
                assertNotNull(actual.response());
                assertEquals(expected, actual.response().userName);
            }
        });

        // 4. Verify the entire batch that failed
        assertEquals(transientlyFailedUserIds.size(), Set.copyOf(transientlyFailedUserIds).size());
        if (totalRequests > UNLUCKY_USER_ID) {
            assertTrue(transientlyFailedUserIds.size() <= 25);
            assertTrue(transientlyFailedUserIds.contains(UNLUCKY_USER_ID));
        }
    }
}