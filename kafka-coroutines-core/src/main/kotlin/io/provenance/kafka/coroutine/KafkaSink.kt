package io.provenance.kafka.coroutine

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

suspend fun <T> Future<T>.asDeferred(timeout: Duration = Duration.ZERO, coroutineContext: CoroutineContext = Dispatchers.IO): Deferred<T> {
    return withContext(coroutineContext) {
        async {
            if (timeout == Duration.ZERO) get()
            else get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        }
    }
}

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
