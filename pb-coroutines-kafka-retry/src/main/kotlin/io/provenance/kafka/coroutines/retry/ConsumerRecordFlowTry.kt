package io.provenance.kafka.coroutines.retry

import io.provenance.coroutines.retry.flow.FlowProcessor
import io.provenance.coroutines.retry.tryOnEachProcess
import io.provenance.coroutines.tryOnEach
import io.provenance.kafka.coroutines.retry.flow.lifted
import io.provenance.kafka.records.UnAckedConsumerRecord
import io.provenance.kafka.records.UnAckedConsumerRecords
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Kafka specific implementation of [tryOnEach] to lift the [UnAckedConsumerRecord] into a basic [ConsumerRecord] for processing.
 *
 * @param flowProcessor The [FlowProcessor] to use for processing the stream of records.
 * @return The original flow.
 */
fun <K, V> Flow<UnAckedConsumerRecords<K, V>>.tryOnEach(
    flowProcessor: FlowProcessor<ConsumerRecord<K, V>>
): Flow<UnAckedConsumerRecords<K, V>> = tryOnEachProcess(flowProcessor.lifted())
