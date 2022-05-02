package io.provenance.kafka.coroutines.retry.store

import io.provenance.kafka.coroutines.retry.KCACHE_RETRY_HEADER
import io.provenance.kafka.coroutines.retry.toByteArray
import java.time.OffsetDateTime
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader

data class ConsumerRecordRetry<K, V>(
    private val initialConsumerRecord: ConsumerRecord<K, V>,
    val attempt: Int = 0,
    val lastAttempted: OffsetDateTime = OffsetDateTime.now()
) {

    val consumerRecord: ConsumerRecord<K, V> = ConsumerRecord(
        initialConsumerRecord.topic(),
        initialConsumerRecord.partition(),
        initialConsumerRecord.offset(),
        initialConsumerRecord.timestamp(),
        initialConsumerRecord.timestampType(),
        initialConsumerRecord.serializedKeySize(),
        initialConsumerRecord.serializedValueSize(),
        initialConsumerRecord.key(),
        initialConsumerRecord.value(),
        initialConsumerRecord.headers().addOrUpdate(
            RecordHeader(KCACHE_RETRY_HEADER, attempt.toByteArray())
        ),
        initialConsumerRecord.leaderEpoch()
    )

    private fun Headers.addOrUpdate(header: Header): Headers {
        val h = find { it.key() == header.key() }
        if (h == null) {
            add(header)
        } else {
            remove(header.key())
            add(header.key(), header.value())
        }
        return this
    }
}
