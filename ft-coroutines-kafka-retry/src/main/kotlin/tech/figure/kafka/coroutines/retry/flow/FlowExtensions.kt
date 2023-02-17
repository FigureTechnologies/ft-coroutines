package tech.figure.kafka.coroutines.retry.flow

import org.apache.kafka.clients.consumer.ConsumerRecord
import tech.figure.coroutines.retry.flow.FlowProcessor
import tech.figure.kafka.coroutines.retry.tryOnEach
import tech.figure.kafka.records.UnAckedConsumerRecord

/**
 * Lift the [FlowProcessor] from [UnAckedConsumerRecord] to [ConsumerRecord] type.
 *
 * This is needed to match the flow type of [tryOnEach] when processing with [KafkaFlowRetry].
 */
fun <K, V> FlowProcessor<List<ConsumerRecord<K, V>>>.lifted(): FlowProcessor<List<UnAckedConsumerRecord<K, V>>> {
    return object : FlowProcessor<List<UnAckedConsumerRecord<K, V>>> {
        override suspend fun send(item: List<UnAckedConsumerRecord<K, V>>, e: Throwable) {
            this@lifted.send(item.map { it.toConsumerRecord() }, e)
        }

        override suspend fun process(item: List<UnAckedConsumerRecord<K, V>>, attempt: Int) {
            this@lifted.process(item.map { it.toConsumerRecord() })
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
