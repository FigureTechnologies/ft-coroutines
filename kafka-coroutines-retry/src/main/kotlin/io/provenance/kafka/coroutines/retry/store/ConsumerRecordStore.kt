package io.provenance.kafka.coroutines.retry.store

import java.time.OffsetDateTime
import org.apache.kafka.clients.consumer.ConsumerRecord

interface ConsumerRecordStore<K, V> {
    suspend fun selectForUpdate(lastAttempted: OffsetDateTime, attemptRange: IntRange): List<ConsumerRecordRetry<K, V>>

    suspend fun getOne(consumerRecord: ConsumerRecord<K, V>): ConsumerRecordRetry<K, V>?

    suspend fun putOne(
        consumerRecord: ConsumerRecord<K, V>,
        mutator: (ConsumerRecordRetry<K, V>) -> ConsumerRecordRetry<K, V> = { it }
    )

    suspend fun remove(consumerRecord: ConsumerRecord<K, V>)
}

internal fun <K, V> recordMatches(other: ConsumerRecord<K, V>): (ConsumerRecordRetry<K, V>) -> Boolean {
    return {
        with(it.consumerRecord) {
            key() == other.key() &&
                value() == other.value() &&
                topic() == other.topic() &&
                partition() == other.partition()
        }
    }
}
