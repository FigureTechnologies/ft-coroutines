package tech.figure.kafka.context

import tech.figure.kafka.records.AckedConsumerRecord

data class AckedConsumerRecordValue<K, V, T>(val record: AckedConsumerRecord<K, V>, val value: T) {
    fun <R> map(block: (T) -> R): AckedConsumerRecordValue<K, V, R> =
        AckedConsumerRecordValue(record, block(value))
}
