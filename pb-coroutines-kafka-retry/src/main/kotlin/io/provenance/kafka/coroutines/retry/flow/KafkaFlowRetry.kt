package io.provenance.kafka.coroutines.retry.flow

import io.provenance.coroutines.retry.store.RetryRecord
import io.provenance.coroutines.retry.flow.FlowRetry
import io.provenance.coroutines.retry.store.RetryRecordStore
import io.provenance.kafka.coroutines.retry.DEFAULT_RECORD_REPROCESS_GROUP_SIZE
import io.provenance.kafka.coroutines.retry.KAFKA_RETRY_ATTEMPTS_HEADER
import io.provenance.kafka.coroutines.retry.toByteArray
import java.time.OffsetDateTime
import kotlinx.coroutines.flow.asFlow
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader

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
        item: ConsumerRecord<K, V>,
        e: Throwable
    ) {
        log.debug { "adding record to retry queue key:${item.key()} source:${item.topic()}-${item.partition()}" }
        store.putOne(item, e) {
            this.apply {
                this.attempt = 0
                this.lastAttempted = OffsetDateTime.now()
                this.lastException = e.localizedMessage

            }
        }
    }

    override suspend fun onSuccess(
        item: RetryRecord<ConsumerRecord<K, V>>
    ) {
        log.debug { "successful reprocess attempt:${item.attempt} key:${item.data.key()} source:${item.data.topic()}-${item.data.partition()}" }
        store.remove(item.data)
    }

    override suspend fun onFailure(item: RetryRecord<ConsumerRecord<K, V>>, e: Throwable) {
        log.debug { "failed reprocess attempt:${item.attempt} Error: ${item.lastException} key:${item.data.key()} source:${item.data.topic()}-${item.data.partition()}" }
        store.putOne(item.data, e) {
            this.apply {
                this.attempt = this.attempt.inc()
                this.lastAttempted = OffsetDateTime.now()
                this.lastException = e.localizedMessage
            }
        }
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

    private fun <K, V> ConsumerRecord<K, V>.setHeader(key: String, value: ByteArray): ConsumerRecord<K, V> = apply {
        fun Headers.addOrUpdate(header: Header): Headers {
            val h = find { it.key() == header.key() }
            if (h == null) {
                add(header)
            } else {
                remove(header.key())
                add(header.key(), header.value())
            }
            return this
        }

        headers().addOrUpdate(RecordHeader(key, value))
    }
}
