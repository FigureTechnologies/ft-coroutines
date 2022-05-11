package io.provenance.kafka.coroutines.retry

import io.provenance.coroutines.tryOnEach as tryOnEach_
import io.provenance.kafka.coroutines.UnAckedConsumerRecord
import io.provenance.kafka.coroutines.retry.flow.lifted
import io.provenance.kafka.coroutines.retry.store.RetryRecord
import java.time.OffsetDateTime
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

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

/**
 * Callback hook for submitting an item into a retry flow.
 */
interface FlowProcessor<T> {
    /**
     * Send an item into this retry flow for later reprocessing
     *
     * @param item The item to send into the retry hopper.
     */
    suspend fun send(item: T)

    /**
     * Process an item.
     *
     * @param item The item that is being retried.
     */
    suspend fun process(item: T, attempt: Int = 0)
}

/**
 * Kafka specific implementation of [tryOnEach] to lift the [UnAckedConsumerRecord] into a basic [ConsumerRecord] for processing.
 *
 * @param flowProcessor The [FlowProcessor] to use for processing the stream of records.
 * @return The original flow.
 */
fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.tryOnEach(
    flowProcessor: FlowProcessor<ConsumerRecord<K, V>>
): Flow<UnAckedConsumerRecord<K, V>> = tryFlow(flowProcessor.lifted())

/**
 * Wrap [onEach] into a try {} catch {} to allow dropping the failed flow element into a [FlowProcessor] for later reprocessing.
 *
 * @param flowProcessor The [FlowProcessor] containing callbacks for processing and error handling.
 * @return The original flow.
 */
fun <T> Flow<T>.tryFlow(flowProcessor: FlowProcessor<T>): Flow<T> = tryOnEach_(
    onFailure = { it, e ->
        KotlinLogging.logger {}.warn("failed to process record", e)
        flowProcessor.send(it)
    },
    tryBlock = {
        flowProcessor.process(it)
    }
)
