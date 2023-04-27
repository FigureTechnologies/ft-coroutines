package tech.figure.kafka.coroutines.retry

import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import tech.figure.coroutines.retry.flow.FlowProcessor
import tech.figure.coroutines.retry.tryOnEachProcess
import tech.figure.coroutines.tryOnEach
import tech.figure.kafka.coroutines.retry.flow.lifted
import tech.figure.kafka.records.UnAckedConsumerRecord

/**
 * Kafka specific implementation of [tryOnEach] to lift the [UnAckedConsumerRecord] into a basic [ConsumerRecord] for processing.
 *
 * @param flowProcessor The [FlowProcessor] to use for processing the stream of records.
 * @return The original flow.
 */
fun <K, V> Flow<List<UnAckedConsumerRecord<K, V>>>.tryOnEach(
    flowProcessor: FlowProcessor<List<ConsumerRecord<K, V>>>,
): Flow<List<UnAckedConsumerRecord<K, V>>> = tryOnEachProcess(flowProcessor.lifted())
