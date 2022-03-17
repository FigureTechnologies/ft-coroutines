package io.provenance.kafka.coroutine

import java.time.Duration
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

interface CommitConsumerRecord {
    val duration: Duration
    val topicPartition: TopicPartition
    val offsetAndMetadata: OffsetAndMetadata
    val commitAck: Channel<Unit>

    fun asCommitable(): Map<TopicPartition, OffsetAndMetadata> = mapOf(topicPartition to offsetAndMetadata)
}

data class CommitConsumerRecordImpl(
    override val duration: Duration,
    override val topicPartition: TopicPartition,
    override val offsetAndMetadata: OffsetAndMetadata,
    override val commitAck: Channel<Unit>,
) : CommitConsumerRecord
