package org.example;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Batches a number of small payloads and write one large object to S3.
 * Precomputes and returns the S3 file location pointer ({@see #BatchLocation} immediately,
 * together with a {@see #CompletableFuture} indicating the individual results (success or failure).
 * It has built-in timeout in case the buffer is idle.
 */
@RequiredArgsConstructor
public class S3BatchWriter extends SequentiallyBufferedBatcher<byte[], Void> {

    private static final int OPTIMAL_BLOB_SIZE_IN_BYTES = 5 * 1024 * 1024;
    private static final Duration TIMEOUT_TO_FLUSH = Duration.ofSeconds(3);

    private final S3Client s3Client;
    private final String bucketName;

    /**
     * Write data to S3 blob asynchronously. Returns precomputed location and a completable future.
     * @param data data to write
     * @return precomputed location in S3 blob, and a response future tracking the end result
     */
    public BatchEntry<Void> precomputeLocationAndWriteAsync(final byte[] data) {
        return handleAsync(data);
    }

    @Override
    protected List<Result<Void>> handleBatch(final List<byte[]> requests, final String batchName) {
        final int totalSize = requests.stream().mapToInt(request -> request.length).sum();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
        requests.stream().forEach(byteBuffer::put);
        byteBuffer.flip();
        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(batchName)
                .contentType("application/octet-stream")
                .build();
        s3Client.putObject(request, RequestBody.fromRemainingByteBuffer(byteBuffer));
        return requests.stream()
                .map(req -> new Result<Void>(null, null))
                .toList();
    }

    @Override
    protected long measureSize(final byte[] request) {
        return request.length;
    }

    @Override
    protected boolean shouldFlush(final long currentBatchOffset) {
        return currentBatchOffset >= OPTIMAL_BLOB_SIZE_IN_BYTES;
    }

    @Override
    protected Duration provideBatchTimeout() {
        return TIMEOUT_TO_FLUSH;
    }

    @Override
    protected String generateBatchName() {
        return UUID.randomUUID().toString();
    }
}
