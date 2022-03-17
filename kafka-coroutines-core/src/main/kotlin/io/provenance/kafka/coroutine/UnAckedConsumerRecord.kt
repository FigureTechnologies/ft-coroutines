package io.provenance.kafka.coroutine

import java.time.Duration
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

interface UnAckedConsumerRecord<K, V> : KafkaRecord<K, V> {
    suspend fun ack(): AckedConsumerRecord<K, V>

    fun <R> withValue(block: (ConsumerRecord<K, V>) -> Pair<R, Int>): UnAckedConsumerRecord<K, R>
}

class UnAckedConsumerRecordImpl<K, V>(
    private val record: ConsumerRecord<K, V>,
    private val channel: SendChannel<CommitConsumerRecord>,
    private val start: Long
) : UnAckedConsumerRecord<K, V>, KafkaRecord<K, V> by wrapping(record) {

    override fun <R> withValue(block: (ConsumerRecord<K, V>) -> Pair<R, Int>): UnAckedConsumerRecord<K, R> {
        val newRecord = record.withValue { block(record) }
        return UnAckedConsumerRecordImpl(newRecord, channel, start)
    }

    override suspend fun ack(): AckedConsumerRecord<K, V> {
        val tp = TopicPartition(record.topic(), record.partition())
        val commit = OffsetAndMetadata(record.offset() + 1)
        val time = System.currentTimeMillis() - start
        val ack = Channel<Unit>()
        channel.send(CommitConsumerRecordImpl(Duration.ofMillis(time), tp, commit, ack)).also {
            ack.receive()
            ack.close()
        }
        return AckedConsumerRecordImpl(record, commit)
    }
}

fun <K, V, R> ConsumerRecord<K, V>.withValue(newValue: (V) -> Pair<R, Int>): ConsumerRecord<K, R> {
    val (newV, size) = newValue(value())
    return ConsumerRecord(
        topic(),
        partition(),
        offset(),
        timestamp(),
        timestampType(),
        serializedKeySize(),
        size,
        key(),
        newV,
        headers(),
        leaderEpoch()
    )
}
