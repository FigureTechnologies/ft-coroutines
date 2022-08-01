package io.provenance.kafka.cli

import ch.qos.logback.classic.Level
import io.provenance.coroutines.retry.flow.retryFlow
import io.provenance.coroutines.retry.tryOnEachProcess
import io.provenance.coroutines.tryOnEach
import io.provenance.kafka.records.acking
import io.provenance.kafka.coroutines.channels.kafkaConsumerChannel
import io.provenance.kafka.coroutines.channels.kafkaProducerChannel
import io.provenance.kafka.coroutines.retry.KAFKA_RETRY_ATTEMPTS_HEADER
import io.provenance.kafka.coroutines.retry.flow.KafkaFlowRetry
import io.provenance.kafka.coroutines.retry.store.inMemoryConsumerRecordStore
import io.provenance.kafka.coroutines.retry.toByteArray
import io.provenance.kafka.coroutines.retry.toInt
import io.provenance.kafka.coroutines.retry.tryOnEach
import kotlin.time.Duration.Companion.days
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

fun main() = runBlocking {
    log {
        "ROOT".level = Level.DEBUG
        "org.apache.kafka.clients".level = Level.WARN
        "org.apache.kafka.common.network".level = Level.WARN
    }

    val log = KotlinLogging.logger {}
    val props = mapOf<String, Any>(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
    )
    val consumerProps = mapOf<String, Any>(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        ConsumerConfig.GROUP_ID_CONFIG to "this-is-a-test",
    )
    val producerProps = mapOf<String, Any>(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
    )

    val o = kafkaConsumerChannel<ByteArray, ByteArray>(props + consumerProps, setOf("input", "out"))
    val i = kafkaProducerChannel<ByteArray, ByteArray>(props + producerProps)
    val retryHandler = KafkaFlowRetry(mapOf("input" to someHandler()), inMemoryConsumerRecordStore())

    launch(Dispatchers.IO) {
        retryFlow(retryHandler).tryOnEach { log.info("successfully processed:$it") }.collect()
    }

    launch(Dispatchers.IO) {
        o.consumeAsFlow().tryOnEach(retryHandler).acking().collect()
    }

    val idx = 1
    i.send(ProducerRecord("input", idx.toByteArray(), "some value".toByteArray()))
    delay(1.days)
}

private fun someHandler(): suspend (ConsumerRecord<ByteArray, ByteArray>) -> Unit = fn@{
    val log = KotlinLogging.logger {}
    val retryAttempt = it.headers().lastHeader(KAFKA_RETRY_ATTEMPTS_HEADER)?.value()?.toInt()

    // Let it pass on attempt 5
    // val index = it.key().toInt()
    if ((retryAttempt ?: 0) > 2) {
        log.warn("forcing succeeding retry")
        return@fn
    }

    // Forced death.
    throw RuntimeException("forced failure")
}
