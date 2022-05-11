package io.provenance.kafka.coroutines.retry.store

import java.time.OffsetDateTime
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

fun <K, V> inMemoryRWStore(data: MutableList<RetryRecord<ConsumerRecord<K, V>>> = mutableListOf()) = object : RetryRecordStore<ConsumerRecord<K, V>> {
    val log = KotlinLogging.logger {}

    override suspend fun select(
        attemptRange: IntRange,
        lastAttempted: OffsetDateTime
    ): List<RetryRecord<ConsumerRecord<K, V>>> {
        return data
            .filter { it.attempt in attemptRange && it.lastAttempted.isBefore(lastAttempted) }
            .sortedBy { it.data.timestamp() }
    }

    override suspend fun getOne(
        item: ConsumerRecord<K, V>
    ): RetryRecord<ConsumerRecord<K, V>>? {
        return data.firstOrNull(recordMatches(item))
    }

    override suspend fun putOne(
        item: ConsumerRecord<K, V>,
        mutator: (RetryRecord<ConsumerRecord<K, V>>) -> RetryRecord<ConsumerRecord<K, V>>
    ) {
        val record = getOne(item)
        if (record == null) {
            data += RetryRecord(item, 0, OffsetDateTime.now()).also {
                log.debug { "putting new entry for $item" }
            }
            return
        }

        data[data.indexOf(record)] = mutator(record).also {
            log.debug { "incrementing attempt for $it" }
        }
    }

    override suspend fun remove(item: ConsumerRecord<K, V>) {
        data.removeAll(recordMatches(item))
    }

    private fun <K, V> recordMatches(other: ConsumerRecord<K, V>): (RetryRecord<ConsumerRecord<K, V>>) -> Boolean {
        return {
            with(it.data) {
                key() == other.key() &&
                    value() == other.value() &&
                    topic() == other.topic() &&
                    partition() == other.partition()
            }
        }
    }
}
