package tech.figure.kafka.context

import tech.figure.kafka.records.UnAckedConsumerRecord

class UnAckedValue<K, V, T>(
    val records: List<UnAckedConsumerRecord<K, V>>,
    val data: T
) {
    suspend fun ack(): AckedValue<K, V, T> =
        AckedValue(records.map { it.ack() }, data)
}
