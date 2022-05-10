package io.provenance.kafka.coroutines.retry

import ch.qos.logback.classic.Level
import io.provenance.kafka.coroutine.UnAckedConsumerRecord
import io.provenance.kafka.coroutine.acking
import io.provenance.kafka.coroutine.kafkaConsumerChannel
import io.provenance.kafka.coroutine.kafkaProducerChannel
import io.provenance.kafka.coroutines.retry.flow.retryFlow
import io.provenance.kafka.coroutines.retry.store.inMemoryRWStore
import java.nio.ByteBuffer
import kotlin.time.Duration.Companion.days
import kotlin.time.ExperimentalTime
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
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

@OptIn(ExperimentalTime::class)
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
    val store = inMemoryRWStore<ByteArray, ByteArray>()
    val handler = someHandler()
    val retryHandler = KafkaFlowRetry(mapOf("input" to handler), store)

    launch(Dispatchers.IO) {
        retryFlow(retryHandler).collect { log.info("successfully processed:$it") }
    }

    launch(Dispatchers.IO) {
        o.consumeAsFlow().tryOnEach(retryHandler).acking().collect()
    }

    val idx = 1
    i.send(ProducerRecord("input", idx.toByteArray(), "some value".toByteArray()))
    delay(1.days)
}

fun Int.toByteArray() =
    ByteBuffer.allocate(Int.SIZE_BYTES).putInt(this).array()

fun ByteArray.toInt() =
    ByteBuffer.wrap(this).int

fun someHandler(): suspend (ConsumerRecord<ByteArray, ByteArray>) -> Unit = fn@{
    val log = KotlinLogging.logger {}
    val retryAttempt = it.headers().lastHeader(KAFKA_RETRY_ATTEMPTS_HEADER)?.value()?.toInt()

    // Let it pass on attempt 5
    // val index = it.key().toInt()
    if ((retryAttempt ?: 0) > 4) {
        log.warn("forcing succeeding retry")
        return@fn
    }

    // Forced death.
    throw RuntimeException("forced failure")
}

fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.tryOnEach(
    flowProcessor: FlowProcessor<ConsumerRecord<K, V>>,
    tryBlock: suspend (value: ConsumerRecord<K, V>) -> Unit = { flowProcessor.process(it) }
): Flow<UnAckedConsumerRecord<K, V>> = tryOnEach(flowProcessor.lifted(), tryBlock.lifted())

