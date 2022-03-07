package io.provenance.kafka.coroutine

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.RecordMetadata
import kotlin.time.Duration
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import org.apache.kafka.clients.consumer.ConsumerRecord

suspend fun <T> Future<T>.asDeferred(timeout: Duration = Duration.ZERO, coroutineContext: CoroutineContext = Dispatchers.IO): Deferred<T> {
    return withContext(coroutineContext) {
        async {
            if (timeout == Duration.ZERO) get()
            else get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        }
    }
}

@OptIn(ExperimentalStdlibApi::class)
class KafkaSink<K, V>(
    producerProps: Map<String, Any>,
    val topicName: String,
    val kafkaProducer: Producer<K, V> = KafkaProducer(producerProps)
) {

    suspend fun send(key: K, block: V) {
        sendHelper(key, block).asDeferred().await()
    }

    fun sendHelper(key: K, block: V): Future<RecordMetadata> {
        return kafkaProducer.send(ProducerRecord(topicName, key, block))
    }
}

private fun <K, V> sendToTopic(producerRecord: ProducerRecord<K, V>, producer: Producer<K, V>, cb: (RecordMetadata) -> Unit): RecordMetadata {
    return producer.send(producerRecord).get().also(cb)
}

suspend fun <T, K, V> Flow<T>.collectToTopic(
    producerProps: Map<String, Any>,
    producer: Producer<K, V> = KafkaProducer(producerProps),
    afterSend: (RecordMetadata) -> Unit = {},
    block: (T) -> ProducerRecord<K, V>
) = collect { sendToTopic(block(it), producer, afterSend) }

fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.onEachToTopic(
    producerProps: Map<String, Any>,
    producer: Producer<K, V> = KafkaProducer(producerProps),
    afterSend: (RecordMetadata) -> Unit = {},
    block: (ConsumerRecord<K, V>) -> ProducerRecord<K, V>
): Flow<UnAckedConsumerRecord<K, V>> = onEach { sendToTopic(block(it.record), producer, afterSend) }
