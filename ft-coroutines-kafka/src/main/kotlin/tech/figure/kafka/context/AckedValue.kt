package tech.figure.kafka.context

import tech.figure.kafka.records.AckedConsumerRecord

class AckedValue<K, V, T>(
    val records: List<AckedConsumerRecord<K, V>>,
    val data: T
)
