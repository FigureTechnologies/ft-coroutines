package io.provenance.kafka.coroutines.retry

import io.provenance.kafka.UnAckedConsumerRecord
import io.provenance.kafka.coroutines.retry.flow.FlowProcessor
import io.provenance.kafka.coroutines.retry.flow.lifted
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import io.provenance.coroutines.tryOnEach as tryOnEach_

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
