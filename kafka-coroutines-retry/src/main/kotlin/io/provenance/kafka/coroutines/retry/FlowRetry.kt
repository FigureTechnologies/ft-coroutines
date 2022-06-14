package io.provenance.kafka.coroutines.retry

import io.provenance.kafka.coroutine.UnAckedConsumerRecord
import io.provenance.kafka.coroutines.retry.store.RetryRecord
import java.time.OffsetDateTime
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform
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
    suspend fun send(item: T, e: Throwable)

    /**
     * Process an item.
     *
     * @param item The item that is being retried.
     */
    suspend fun process(item: T, attempt: Int = 0)
}

/**
 * Generic logging flow error handler.
 */
fun <T> logErrorFlowHandler(): suspend (value: T, throwable: Throwable) -> Unit {
    val log = KotlinLogging.logger {}
    return { value, throwable ->
        log.error("Failed to handle $value", throwable)
    }
}

/**
 * Kafka specific implementation of [tryOnEach] to lift the [UnAckedConsumerRecord] into a basic [ConsumerRecord] for processing.
 *
 * @param flowProcessor The [FlowProcessor] to use for processing the stream of records.
 * @return The original flow.
 */
fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.tryOnEachConsumerRecord(
    flowProcessor: FlowProcessor<ConsumerRecord<K, V>>
): Flow<UnAckedConsumerRecord<K, V>> = tryOnEach(flowProcessor.lifted())

/**
 * Wrap [onEach] into a try {} catch {} to allow dropping the failed flow element into a [FlowProcessor] for later reprocessing.
 *
 * @param flowProcessor The [FlowProcessor] containing callbacks for processing and error handling.
 * @return The original flow.
 */
fun <T> Flow<T>.tryOnEach(
    flowProcessor: FlowProcessor<T>
): Flow<T> = tryOnEach(
    onFailure = { it, e ->
        KotlinLogging.logger {}.warn("failed to process record", e)
        flowProcessor.send(it, e)
    },
    tryBlock = {
        flowProcessor.process(it)
    }
)

/**
 * Wrap [onEach] into a try {} catch {} to allow dropping the failed flow element into the [onFailure] handler for later reprocessing.
 *
 * @param onFailure The method used to submit the failed element into retry flow.
 * @param tryBlock The method to use to initially process the flow element.
 * @return The original flow.
 */
fun <T> Flow<T>.tryOnEach(
    onFailure: suspend (value: T, throwable: Throwable) -> Unit = logErrorFlowHandler(),
    tryBlock: suspend (value: T) -> Unit,
): Flow<T> = onEach { value: T ->
    runCatching { tryBlock(value) }.fold(
        onSuccess = { },
        onFailure = { onFailure(value, it) }
    )
}

/**
 * Wrap [transform] into a try {} catch {} to allow dropping the failed flow element into the [onFailure] handler for later reprocessing.
 *
 * @param onFailure The method used to submit the failed element into retry flow.
 * @param tryBlock The method to use to initially process the flow element.
 * @return A [Flow] of successful output from [tryBlock].
 */
fun <T, R> Flow<T>.tryMap(
    onFailure: suspend (value: T, throwable: Throwable) -> Unit = logErrorFlowHandler(),
    tryBlock: suspend (value: T) -> R,
): Flow<R> = transform { value: T ->
    runCatching { tryBlock(value) }.fold(
        onSuccess = { emit(it) },
        onFailure = {
            runCatching { onFailure(value, it) }.fold(
                onSuccess = {},
                onFailure = {},
            )
        },
    )
}
