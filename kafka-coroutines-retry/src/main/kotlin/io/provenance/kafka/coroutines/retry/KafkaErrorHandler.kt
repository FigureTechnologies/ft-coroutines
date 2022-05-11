package io.provenance.kafka.coroutines.retry

import io.provenance.kafka.coroutine.UnAckedConsumerRecord
import io.provenance.kafka.coroutines.retry.store.RetryRecord
import io.provenance.kafka.coroutines.retry.store.RetryRecordStore
import io.provenance.kafka.coroutines.retry.store.setHeader
import java.time.OffsetDateTime
import kotlinx.coroutines.flow.asFlow
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

const val DEFAULT_RECORD_REPROCESS_GROUP_SIZE = 10
const val KAFKA_RETRY_ATTEMPTS_HEADER = "kcache-retry"

/**
 * Lift the [FlowProcessor] from [UnAckedConsumerRecord] to [ConsumerRecord] type.
 *
 * This is needed to match the flow type of [tryOnEach] when processing with [KafkaFlowRetry].
 */
fun <K, V> FlowProcessor<ConsumerRecord<K, V>>.lifted(): FlowProcessor<UnAckedConsumerRecord<K, V>> {
    return object : FlowProcessor<UnAckedConsumerRecord<K, V>> {
        override suspend fun send(item: UnAckedConsumerRecord<K, V>) {
            this@lifted.send(item.toConsumerRecord())
        }

        override suspend fun process(item: UnAckedConsumerRecord<K, V>, attempt: Int) {
            this@lifted.process(item.toConsumerRecord())
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

/**
 * Retry a flow of kafka records.
 *
 * @param handlers The topic-based handlers to reprocess with.
 * @param store [RetryRecordStore] to save and retrieve [ConsumerRecord] from.
 * @param groupSize Process a max of this many elements each poll loop.
 */
open class KafkaFlowRetry<K, V>(
    private val handlers: Map<String, suspend (ConsumerRecord<K, V>) -> Unit>,
    private val store: RetryRecordStore<ConsumerRecord<K, V>>,
    private val groupSize: Int = DEFAULT_RECORD_REPROCESS_GROUP_SIZE,
) : FlowRetry<ConsumerRecord<K, V>> {
    private val log = KotlinLogging.logger {}

    override suspend fun produceNext(
        attemptRange: IntRange,
        olderThan: OffsetDateTime
    ) = store.select(attemptRange, olderThan).sortedByDescending { it.lastAttempted }.take(groupSize).asFlow()

    override suspend fun send(
        item: ConsumerRecord<K, V>
    ) {
        log.debug { "adding record to retry queue key:${item.key()} source:${item.topic()}-${item.partition()}" }
        store.putOne(item) { it.copy(attempt = 0, lastAttempted = OffsetDateTime.now()) }
    }

    override suspend fun onSuccess(
        item: RetryRecord<ConsumerRecord<K, V>>
    ) {
        log.debug { "successful reprocess attempt:${item.attempt} key:${item.data.key()} source:${item.data.topic()}-${item.data.partition()}" }
        store.remove(item.data)
    }

    override suspend fun onFailure(
        item: RetryRecord<ConsumerRecord<K, V>>
    ) {
        log.debug { "failed reprocess attempt:${item.attempt} key:${item.data.key()} source:${item.data.topic()}-${item.data.partition()}" }
        store.putOne(item.data) { it.copy(attempt = it.attempt.inc(), lastAttempted = OffsetDateTime.now()) }
    }

    override suspend fun process(
        item: ConsumerRecord<K, V>,
        attempt: Int,
    ) {
        val topic = item.topic()
        val handler = handlers[topic] ?: throw RuntimeException("topic '$topic' not handled by this retry handler")

        log.debug { "processing key:${item.key()} attempt:$attempt source:${item.topic()}-${item.partition()}" }
        handler(item.setHeader(KAFKA_RETRY_ATTEMPTS_HEADER, attempt.toByteArray()))
    }
}
