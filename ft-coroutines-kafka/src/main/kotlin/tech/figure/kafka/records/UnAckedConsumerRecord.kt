package tech.figure.kafka.records

import java.time.Duration
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
 * Model a [ConsumerRecord] received from kafka but not yet acknowledged.
 */
interface UnAckedConsumerRecord<K, V> : KafkaRecord<K, V> {
    /**
     * Acknowledge the receipt and processing of this record back to the broker.
     *
     * @return The [AckedConsumerRecord].
     */
    suspend fun ack(): AckedConsumerRecord<K, V>
}

/**
 * Wrap a [ConsumerRecord] with contextual data, allowing the user to `ack` back to the broker once processing is done.
 *
 * @param record The [ConsumerRecord] to wrap
 * @param channel The [Channel] to notify once the record is acknowledged via the broker.
 * @param start The [ConsumerRecord.timestamp] for when this record was received from the broker.
 */
class UnAckedConsumerRecordImpl<K, V>(
    private val record: ConsumerRecord<K, V>,
    private val channel: SendChannel<CommitConsumerRecord>,
    private val start: Long
) : UnAckedConsumerRecord<K, V>, KafkaRecord<K, V> by wrapping(record) {

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

    override fun toString(): String =
        """
        UnAckedConsumerRecord(topic:$topic
            partition:$partition
            leaderEpoch:$leaderEpoch
            offset:$offset
            $timestampType:$timestamp
            serialized key size:$serializedKeySize
            serialized value size:$serializedValueSize
            headers:$headers
            key:$key
            value:$value)
        """.trimIndent().split('\n').joinToString { it.trim() }
}

@JvmInline
value class UnAckedConsumerRecords<K, V>(val records: List<UnAckedConsumerRecord<K, V>>)
