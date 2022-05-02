package io.provenance.kafka.coroutines.retry.store

import java.time.OffsetDateTime
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

fun <K, V> inMemoryRWStore(data: MutableList<ConsumerRecordRetry<K, V>> = mutableListOf()): ConsumerRecordStore<K, V> {
    return object : ConsumerRecordStore<K, V> {
        private val log = KotlinLogging.logger {}

        override suspend fun selectForUpdate(lastAttempted: OffsetDateTime, attemptRange: IntRange) =
            data.filter { it.attempt in attemptRange && it.lastAttempted.isBefore(lastAttempted) }
                .sortedBy { it.consumerRecord.timestamp() }

        override suspend fun getOne(consumerRecord: ConsumerRecord<K, V>): ConsumerRecordRetry<K, V>? {
            return data.firstOrNull(recordMatches(consumerRecord))
        }

        override suspend fun putOne(
            consumerRecord: ConsumerRecord<K, V>,
            mutator: (ConsumerRecordRetry<K, V>) -> ConsumerRecordRetry<K, V>
        ) {
            val record = getOne(consumerRecord)
            if (record == null) {
                data += ConsumerRecordRetry(consumerRecord).also {
                    log.debug { "putting new entry for $it" }
                }
                return
            }

            data[data.indexOf(record)] = mutator(record).also {
                log.debug { "incrementing attempt for $it" }
            }
        }

        override suspend fun remove(consumerRecord: ConsumerRecord<K, V>) {
            data.removeAll(recordMatches(consumerRecord))
        }
    }
}
