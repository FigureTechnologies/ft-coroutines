package io.provenance.kafka.coroutines.retry.flow

import io.provenance.kafka.coroutines.retry.store.RetryRecord
import java.time.OffsetDateTime
import kotlinx.coroutines.flow.Flow

/**
 * Callbacks and hooks to generate a limited [Flow] of [RetryRecord] to retry.
 */
interface FlowRetry<T> : FlowProcessor<T> {
    /**
     * Generate the next group of items to retry.
     *
     * @param attemptRange Only select records with [RetryRecord.attempt] in [attemptRange].
     * @param olderThan Only select records with [RetryRecord.lastAttempted] being before [olderThan].
     * @return A [Flow] of [RetryRecord] to process and feed into [process].
     */
    suspend fun produceNext(attemptRange: IntRange, olderThan: OffsetDateTime): Flow<RetryRecord<T>>

    /**
     * Callback executed after successful processing of [process].
     *
     * @param item The item that was successfully retried.
     */
    suspend fun onSuccess(item: RetryRecord<T>)

    /**
     * Callback executed after failed processing of [process].
     *
     * @param item The item that was unsuccessfully retried.
     */
    suspend fun onFailure(item: RetryRecord<T>)
}
