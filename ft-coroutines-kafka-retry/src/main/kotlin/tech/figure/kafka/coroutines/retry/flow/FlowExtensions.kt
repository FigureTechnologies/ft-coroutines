package tech.figure.kafka.coroutines.retry.flow

import tech.figure.coroutines.retry.flow.FlowProcessor
import tech.figure.kafka.coroutines.retry.tryOnEach
import tech.figure.kafka.records.UnAckedConsumerRecord
import tech.figure.kafka.records.UnAckedConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Lift the [FlowProcessor] from [UnAckedConsumerRecord] to [ConsumerRecord] type.
 *
 * This is needed to match the flow type of [tryOnEach] when processing with [KafkaFlowRetry].
 */
fun <K, V> FlowProcessor<ConsumerRecord<K, V>>.lifted(): FlowProcessor<UnAckedConsumerRecords<K, V>> {
    return object : FlowProcessor<UnAckedConsumerRecords<K, V>> {
        override suspend fun send(item: UnAckedConsumerRecords<K, V>, e: Throwable) {
            item.forEach { this@lifted.send(it.toConsumerRecord(), e) }
        }

        override suspend fun process(item: UnAckedConsumerRecords<K, V>, attempt: Int) {
            item.forEach { this@lifted.process(it.toConsumerRecord()) }
        }
    }
}

/**
 * Lift the lambda callback from [UnAckedConsumerRecord] to [ConsumerRecord] type.
 *
 * This is needed to match the flow type of [tryOnEach] when processing with [KafkaFlowRetry].
 */
fun <K, V> (suspend (ConsumerRecord<K, V>) -> Unit).lifted(): suspend (UnAckedConsumerRecord<K, V>) -> Unit =
    { this(it.toConsumerRecord()) }
