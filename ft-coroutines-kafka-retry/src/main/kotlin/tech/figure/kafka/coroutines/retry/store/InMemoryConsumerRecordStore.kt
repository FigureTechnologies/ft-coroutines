package tech.figure.kafka.coroutines.retry.store

import java.time.OffsetDateTime
import org.apache.kafka.clients.consumer.ConsumerRecord
import tech.figure.coroutines.retry.store.RetryRecord
import tech.figure.coroutines.retry.store.RetryRecordStore

fun <K, V> inMemoryConsumerRecordStore(
    data: MutableList<RetryRecord<ConsumerRecord<K, V>>> = mutableListOf()
) = object : RetryRecordStore<ConsumerRecord<K, V>> {
    override suspend fun isEmpty(): Boolean =
        data.isEmpty()

    override suspend fun select(
        attemptRange: IntRange,
        lastAttempted: OffsetDateTime,
        limit: Int,
    ): List<RetryRecord<ConsumerRecord<K, V>>> {
        return data
            .filter { it.attempt in attemptRange && it.lastAttempted.isBefore(lastAttempted) }
            .sortedBy { it.data.timestamp() }
            .take(limit)
    }

    override suspend fun get(
        item: ConsumerRecord<K, V>
    ): RetryRecord<ConsumerRecord<K, V>>? {
        return data.firstOrNull(recordMatches(item))
    }

    override suspend fun insert(item: ConsumerRecord<K, V>, e: Throwable?) {
        data += RetryRecord(item, lastException = e?.localizedMessage.orEmpty())
    }

    override suspend fun update(item: ConsumerRecord<K, V>, e: Throwable?) {
        val record = get(item) ?: error("record not found")

        // Find and update the record in the data set.
        data[data.indexOf(record)].also {
            it.data = item
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
