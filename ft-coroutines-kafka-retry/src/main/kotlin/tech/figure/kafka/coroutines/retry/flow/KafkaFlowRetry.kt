package tech.figure.kafka.coroutines.retry.flow

import java.time.OffsetDateTime
import kotlinx.coroutines.flow.asFlow
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import tech.figure.coroutines.retry.flow.FlowRetry
import tech.figure.coroutines.retry.store.RetryRecord
import tech.figure.coroutines.retry.store.RetryRecordStore
import tech.figure.kafka.coroutines.retry.DEFAULT_RECORD_REPROCESS_GROUP_SIZE
import tech.figure.kafka.coroutines.retry.KAFKA_RETRY_ATTEMPTS_HEADER
import tech.figure.kafka.coroutines.retry.toByteArray

/**
 * Retry a flow of kafka records.
 *
 * @param handlers The topic-based handlers to reprocess with.
 * @param store [RetryRecordStore] to save and retrieve [ConsumerRecord] from.
 * @param groupSize Process a max of this many elements each poll loop.
 */
open class KafkaFlowRetry<K, V>(
    private val handlers: Map<String, suspend (List<ConsumerRecord<K, V>>) -> Unit>,
    private val store: RetryRecordStore<List<ConsumerRecord<K, V>>>,
    private val groupSize: Int = DEFAULT_RECORD_REPROCESS_GROUP_SIZE,
) : FlowRetry<List<ConsumerRecord<K, V>>> {
    private val log = KotlinLogging.logger {}

    override suspend fun hasNext(): Boolean = !store.isEmpty()

    override suspend fun produceNext(
        attemptRange: IntRange,
        olderThan: OffsetDateTime,
        limit: Int,
    ) =
        store
            .select(attemptRange, olderThan, limit)
            .sortedByDescending { it.lastAttempted }
            .asFlow()

    override suspend fun send(item: List<ConsumerRecord<K, V>>, e: Throwable) {
        log.debug {
            "adding record to retry queue count:${item.size} sources:${item.map { "${it.topic()}-${it.partition()}" } }"
        }
        store.insert(item, e)
    }

    override suspend fun onSuccess(item: RetryRecord<List<ConsumerRecord<K, V>>>) {
        log.debug {
            "successful reprocess attempt:${item.attempt} count:${item.data.size} sources:${item.data.map { "${it.topic()}-${it.partition()}" } }"
        }
        store.remove(item.data)
    }

    override suspend fun onFailure(item: RetryRecord<List<ConsumerRecord<K, V>>>, e: Throwable) {
        log.debug {
            "failed reprocess attempt:${item.attempt} Error: ${item.lastException} count:${item.data.size} sources:${item.data.map { "${it.topic()}-${it.partition()}" } }"
        }
        store.update(item.data, e)
    }

    override suspend fun process(
        item: List<ConsumerRecord<K, V>>,
        attempt: Int,
    ) {
        val topic = item.firstOrNull()?.topic() ?: return
        val handler =
            handlers[topic]
                ?: throw RuntimeException("topic '$topic' not handled by this retry handler")

        log.debug {
            "processing count:${item.size} sources:${item.map { "${it.topic()}-${it.partition()}" } }"
        }
        handler(item.map { it.setHeader(KAFKA_RETRY_ATTEMPTS_HEADER, attempt.toByteArray()) })
    }

    private fun <K, V> ConsumerRecord<K, V>.setHeader(
        key: String,
        value: ByteArray
    ): ConsumerRecord<K, V> = apply {
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
