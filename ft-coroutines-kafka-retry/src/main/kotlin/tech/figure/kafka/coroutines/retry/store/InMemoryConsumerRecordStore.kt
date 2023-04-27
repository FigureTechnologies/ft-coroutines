package tech.figure.kafka.coroutines.retry.store

import java.time.OffsetDateTime
import org.apache.kafka.clients.consumer.ConsumerRecord
import tech.figure.coroutines.retry.store.RetryRecord
import tech.figure.coroutines.retry.store.RetryRecordStore

fun <K, V> inMemoryConsumerRecordStore(
    data: MutableList<RetryRecord<List<ConsumerRecord<K, V>>>> = mutableListOf(),
) =
    object : RetryRecordStore<List<ConsumerRecord<K, V>>> {
        override suspend fun isEmpty(): Boolean = data.isEmpty()

        override suspend fun select(
            attemptRange: IntRange,
            lastAttempted: OffsetDateTime,
            limit: Int,
        ): List<RetryRecord<List<ConsumerRecord<K, V>>>> {
            return data
                .filter { it.attempt in attemptRange && it.lastAttempted.isBefore(lastAttempted) }
                .sortedBy { it.data.firstOrNull()?.timestamp() }
                .take(limit)
        }

        override suspend fun get(
            item: List<ConsumerRecord<K, V>>,
        ): RetryRecord<List<ConsumerRecord<K, V>>>? {
            return data.firstOrNull(recordMatches(item))
        }

        override suspend fun insert(item: List<ConsumerRecord<K, V>>, e: Throwable?) {
            data += RetryRecord(item, lastException = e?.localizedMessage.orEmpty())
        }

        override suspend fun update(item: List<ConsumerRecord<K, V>>, e: Throwable?) {
            val record = get(item) ?: error("record not found")

            // Find and update the record in the data set.
            data[data.indexOf(record)].also {
                it.data = item
                it.attempt++
                it.lastAttempted = OffsetDateTime.now()
                it.lastException = e?.localizedMessage.orEmpty()
            }
        }

        override suspend fun remove(item: List<ConsumerRecord<K, V>>) {
            data.removeAll(recordMatches(item))
        }

        private fun <K, V> recordMatches(
            other: List<ConsumerRecord<K, V>>,
        ): (RetryRecord<List<ConsumerRecord<K, V>>>) -> Boolean {
            return { it == other }
        }
    }
