package tech.figure.kafka.context

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import tech.figure.kafka.records.UnAckedConsumerRecord

fun <K, V, T> UnAckedConsumerRecord<K, V>.withValue(t: T) = UnAckedConsumerRecordValue(this, t)

fun <K, V, T> Flow<List<UnAckedConsumerRecordValue<K, V, T>>>.acking(): Flow<List<AckedConsumerRecordValue<K, V, T>>> =
    map { it.map { it.ack()  } }

data class UnAckedConsumerRecordValue<K, V, T>(val record: UnAckedConsumerRecord<K, V>, val value: T) {
    suspend fun ack(): AckedConsumerRecordValue<K, V, T> =
        AckedConsumerRecordValue(record.ack(), value)

    fun <R> map(block: (T) -> R): UnAckedConsumerRecordValue<K, V, R> =
        UnAckedConsumerRecordValue(record, block(value))
}