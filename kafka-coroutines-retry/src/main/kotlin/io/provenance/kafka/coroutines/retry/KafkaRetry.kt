package io.provenance.kafka.coroutines.retry

import io.provenance.kafka.coroutines.retry.store.ConsumerRecordRetry
import java.time.OffsetDateTime
import org.apache.kafka.clients.consumer.ConsumerRecord

interface KafkaRetry<K, V> : FlowReceiver<ConsumerRecord<K, V>> {
    suspend fun fetchNext(count: Int, attemptRange: IntRange, olderThan: OffsetDateTime): List<ConsumerRecordRetry<K, V>>
    suspend fun markComplete(rec: ConsumerRecord<K, V>)
    suspend fun markAttempted(consumerRecord: ConsumerRecord<K, V>)
    suspend fun retry(consumerRecord: ConsumerRecord<K, V>, attempt: Int)
}

interface FlowReceiver<T> {
    suspend fun send(item: T)
}
