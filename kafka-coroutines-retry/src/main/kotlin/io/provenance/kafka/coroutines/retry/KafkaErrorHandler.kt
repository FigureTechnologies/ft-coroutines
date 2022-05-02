package io.provenance.kafka.coroutines.retry

import io.provenance.kafka.coroutines.retry.store.ConsumerRecordRetry
import io.provenance.kafka.coroutines.retry.store.ConsumerRecordStore
import java.time.OffsetDateTime
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

open class KafkaErrorHandler<K, V>(
    private val handlers: Map<String, suspend (ConsumerRecord<K, V>) -> Unit>,
    private val store: ConsumerRecordStore<K, V>,
): KafkaRetry<K, V> {

    private val log = KotlinLogging.logger {}

    override suspend fun send(item: ConsumerRecord<K, V>) {
        store.putOne(item) {
            it.copy(
                attempt = 0,
                lastAttempted = OffsetDateTime.now()
            )
        }
    }

    override suspend fun retry(consumerRecord: ConsumerRecord<K, V>, attempt: Int) {
        val topic = consumerRecord.topic()
        val handler = handlers[topic] ?: throw RuntimeException("topic '$topic' not handled by this retry handler")

        log.info { "Handling retry attempt:$attempt" }
        handler(consumerRecord)
    }

    override suspend fun fetchNext(count: Int, attemptRange: IntRange, olderThan: OffsetDateTime): List<ConsumerRecordRetry<K, V>> {
        return store.selectForUpdate(olderThan, attemptRange).take(count)
    }

    override suspend fun markComplete(rec: ConsumerRecord<K, V>) {
        store.remove(rec)
    }

    override suspend fun markAttempted(consumerRecord: ConsumerRecord<K, V>) {
        store.putOne(consumerRecord) {
            it.copy(
                attempt = it.attempt.inc(),
                lastAttempted = OffsetDateTime.now(),
            )
        }
    }
}

