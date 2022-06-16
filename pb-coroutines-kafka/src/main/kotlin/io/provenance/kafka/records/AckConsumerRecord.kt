package io.provenance.kafka.records

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata

interface AckedConsumerRecord<K, V> : KafkaRecord<K, V> {
    val metadata: OffsetAndMetadata
}

class AckedConsumerRecordImpl<K, V>(
    record: ConsumerRecord<K, V>,
    override val metadata: OffsetAndMetadata
) : AckedConsumerRecord<K, V>, KafkaRecord<K, V> by wrapping(record)
