package tech.figure.kafka.cli

import ch.qos.logback.classic.Level
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.consumeAsFlow
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
import tech.figure.coroutines.retry.flow.retryFlow
import tech.figure.coroutines.tryOnEach
import tech.figure.kafka.coroutines.channels.kafkaConsumerChannel
import tech.figure.kafka.coroutines.channels.kafkaProducerChannel
import tech.figure.kafka.coroutines.retry.KAFKA_RETRY_ATTEMPTS_HEADER
import tech.figure.kafka.coroutines.retry.flow.KafkaFlowRetry
import tech.figure.kafka.coroutines.retry.store.inMemoryConsumerRecordStore
import tech.figure.kafka.coroutines.retry.toByteArray
import tech.figure.kafka.coroutines.retry.toInt
import tech.figure.kafka.coroutines.retry.tryOnEach
import tech.figure.kafka.records.acking

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
    while (true) {
        i.send(ProducerRecord("input", idx.toByteArray(), "some value".toByteArray()))
        delay(4.seconds)
    }
}

private fun someHandler(): suspend (List<ConsumerRecord<ByteArray, ByteArray>>) -> Unit = fn@{
    val log = KotlinLogging.logger {}
    val firstRec = it.firstOrNull() ?: return@fn
    val retryAttempt = firstRec.headers().lastHeader(KAFKA_RETRY_ATTEMPTS_HEADER)?.value()?.toInt()

    // Let it pass on attempt 5
    // val index = it.key().toInt()
    if ((retryAttempt ?: 0) > 2) {
        log.warn("forcing succeeding retry")
        return@fn
    }

    // Forced death.
    throw RuntimeException("forced failure")
}
