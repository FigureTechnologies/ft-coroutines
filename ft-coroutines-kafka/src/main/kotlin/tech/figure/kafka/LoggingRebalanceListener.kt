package tech.figure.kafka

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.event.Level

fun loggingConsumerRebalanceListener(level: Level = Level.DEBUG): ConsumerRebalanceListener = object : ConsumerRebalanceListener {
    private val log = KotlinLogging.logger {}

    val fn: (message: String) -> Unit = when (level) {
        Level.TRACE -> log::trace
        Level.DEBUG -> log::debug
        Level.INFO -> log::info
        Level.WARN -> log::warn
        Level.ERROR -> log::error
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
        fn("partitions assigned: $partitions")
    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        fn("partitions revoked: $partitions")
    }
}
