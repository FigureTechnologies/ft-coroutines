package io.provenance.kafka.records

import java.util.Optional
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

interface KafkaRecord<K, V> {
    val topic: String
    val partition: Int
    val offset: Long
    val key: K
    val value: V
    val headers: List<Header>
    val timestamp: Long
    val timestampType: TimestampType
    val leaderEpoch: Int?
    val serializedKeySize: Int
    val serializedValueSize: Int

    fun toConsumerRecord(): ConsumerRecord<K, V> {
        return ConsumerRecord(
            topic,
            partition,
            offset,
            timestamp,
            timestampType,
            serializedKeySize,
            serializedValueSize,
            key,
            value,
            RecordHeaders(headers),
            leaderEpoch.toOptional()
        )
    }
}

internal fun <K, V> wrapping(consumerRecord: ConsumerRecord<K, V>): KafkaRecord<K, V> {
    return object : KafkaRecord<K, V> {
        override val topic: String = consumerRecord.topic()
        override val key: K = consumerRecord.key()
        override val value: V = consumerRecord.value()
        override val headers: List<Header> = consumerRecord.headers().toList()
        override val partition: Int = consumerRecord.partition()
        override val offset: Long = consumerRecord.offset()
        override val timestamp: Long = consumerRecord.timestamp()
        override val timestampType: TimestampType = consumerRecord.timestampType()
        override val leaderEpoch: Int? = consumerRecord.leaderEpoch().orElseGet { null }
        override val serializedKeySize: Int = consumerRecord.serializedKeySize()
        override val serializedValueSize: Int = consumerRecord.serializedValueSize()
    }
}

private fun <T : Any> T?.toOptional(): Optional<T> =
    if (this == null) Optional.empty()
    else Optional.of(this)
