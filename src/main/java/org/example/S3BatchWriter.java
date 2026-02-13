package org.example;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;

/**
 * Batches a number of small payloads and write one large object to S3.
 * Returns the S3 file location pointer with key and offsets.
 * It has built-in timeout in case the buffer is idle.
 */
@RequiredArgsConstructor
public class S3BatchWriter extends SequentiallyBufferedBatcher<byte[], S3BatchWriter.S3Location> {

    private static final int OPTIMAL_BLOB_SIZE_IN_BYTES = 5 * 1024 * 1024;
    private static final Duration TIMEOUT_TO_FLUSH = Duration.ofSeconds(3);

    private final S3Client s3Client;
    private final String bucketName;

    /**
     * S3 location pointer
     * @param key S3 object key
     * @param startOffset start offset in bytes (inclusive)
     * @param endOffset end offset in bytes (exclusive)
     */
    public record S3Location(String key, int startOffset, int endOffset) {}

    /**
     * Write data to S3 blob in batch. Returns the S3 chunk file location.
     * @param data data to write
     * @return S3 location pointer with key and offsets
     */
    public S3Location putObject(final byte[] data) {
        try {
            return handleAsync(data).join();
        } catch (final CompletionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    @Override
    protected List<Result<S3Location>> handleBatch(final List<byte[]> requests) {
        final String s3ObjectKey = generateBatchName();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getTotalSize(requests));
        final List<S3Location> s3Locations = requests.stream().map(request -> {
            final int startOffset = byteBuffer.position();
            byteBuffer.put(request);
            final int endOffset = byteBuffer.position();
            return new S3Location(s3ObjectKey, startOffset, endOffset);
        }).toList();
        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3ObjectKey)
                .contentType("application/octet-stream")
                .build();
        try {
            s3Client.putObject(request, RequestBody.fromBytes(byteBuffer.array()));
            return s3Locations.stream()
                    .map(s3Location -> new Result<>(s3Location, null))
                    .toList();
        } catch (final Exception exception) {
            return s3Locations.stream()
                    .map(_ -> new Result<>((S3Location) null, exception))
                    .toList();
        }
    }

    @Override
    protected boolean shouldFlush(final List<byte[]> requests) {
        return getTotalSize(requests) >= OPTIMAL_BLOB_SIZE_IN_BYTES;
    }

    @Override
    protected Duration provideBatchTimeout() {
        return TIMEOUT_TO_FLUSH;
    }

    private String generateBatchName() {
        return UUID.randomUUID().toString();
    }

    private int getTotalSize(final List<byte[]> requests) {
        return requests.stream().mapToInt(request -> request.length).sum();
    }
}
