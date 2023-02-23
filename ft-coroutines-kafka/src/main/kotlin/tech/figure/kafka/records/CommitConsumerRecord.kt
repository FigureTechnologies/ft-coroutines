package tech.figure.kafka.records

import java.time.Duration
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

interface CommitConsumerRecord {
    val duration: Duration
    val topicPartition: TopicPartition
    val offsetAndMetadata: OffsetAndMetadata
    val commitAck: SendChannel<Unit>

    fun asCommitable(): Map<TopicPartition, OffsetAndMetadata> = mapOf(topicPartition to offsetAndMetadata)
}

data class CommitConsumerRecordImpl(
    override val duration: Duration,
    override val topicPartition: TopicPartition,
    override val offsetAndMetadata: OffsetAndMetadata,
    override val commitAck: SendChannel<Unit>,
) : CommitConsumerRecord
