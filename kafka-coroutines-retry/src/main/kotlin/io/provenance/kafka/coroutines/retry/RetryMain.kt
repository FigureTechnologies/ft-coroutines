package io.provenance.kafka.coroutines.retry

import ch.qos.logback.classic.Level
import io.provenance.kafka.coroutine.UnAckedConsumerRecord
import io.provenance.kafka.coroutine.acking
import io.provenance.kafka.coroutine.kafkaConsumerChannel
import io.provenance.kafka.coroutine.kafkaProducerChannel
import io.provenance.kafka.coroutines.retry.flow.retryKafkaFlow
import io.provenance.kafka.coroutines.retry.store.inMemoryRWStore
import java.nio.ByteBuffer
import kotlin.time.Duration.Companion.days
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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

var Logger.level: Level
    get() = (this as ch.qos.logback.classic.Logger).level
    set(value) { (this as ch.qos.logback.classic.Logger).level = value }

object LoggerDsl {
    var String.level: Level
        get() = LoggerFactory.getLogger(this).level
        set(value) { LoggerFactory.getLogger(this).level = value }
}

private fun log(block: LoggerDsl.() -> Unit) = LoggerDsl.block()

const val KCACHE_RETRY_HEADER = "kcache-retry"



@OptIn(ExperimentalTime::class)
fun main() = runBlocking {
    log {
        Logger.ROOT_LOGGER_NAME.level = Level.INFO
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
    val retryHandler = KafkaErrorHandler(mapOf("input" to handler), store)

    launch(Dispatchers.IO) {
        retryKafkaFlow(retryHandler).collect { log.info("successfully processed:$it") }
    }

    launch(Dispatchers.IO) {
        o.consumeAsFlow().tryOnEach(retryHandler.lift(), handler.lift()).acking().collect()
    }

    val idx = 1
    i.send(ProducerRecord("input", idx.toByteArray(), "some value".toByteArray()))
    delay(1.days)
}

fun Int.toByteArray() =
    ByteBuffer.allocate(Int.SIZE_BYTES).putInt(this).array()

fun ByteArray.toInt() =
    ByteBuffer.wrap(this).int

fun <K, V> KafkaRetry<K, V>.lift(): suspend (UnAckedConsumerRecord<K, V>, Throwable) -> Unit =
    { record, _ -> send(record.toConsumerRecord()) }

fun <K, V> (suspend (ConsumerRecord<K, V>) -> Unit).lift(): suspend (UnAckedConsumerRecord<K, V>) -> Unit =
    { this(it.toConsumerRecord()) }

fun someHandler(): suspend (ConsumerRecord<ByteArray, ByteArray>) -> Unit = fn@{
    val log = KotlinLogging.logger {}
    val retryAttempt = it.headers().firstOrNull { it.key() == KCACHE_RETRY_HEADER }?.value()?.toInt()

    // Let it pass on attempt 5
    // val index = it.key().toInt()
    if ((retryAttempt ?: 0) > 4) {
        log.info("forcing succeeding retry")
        return@fn
    }

    // Forced death.
    throw RuntimeException("forced failure")
}
