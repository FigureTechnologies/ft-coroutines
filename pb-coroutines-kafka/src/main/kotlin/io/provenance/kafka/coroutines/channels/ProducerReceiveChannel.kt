package io.provenance.kafka.coroutines.channels

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.SelectClause2
import kotlinx.coroutines.selects.SelectInstance
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Create a [SendChannel] for producer records to send to kafka.
 *
 * @param producerProps Kafka producer settings for this channel.
 * @param producer The instantiated [Producer] to use to send to kafka.
 * @return A [KafkaProducerChannel] instance to use to send [ProducerRecord] to Kafka.
 */
fun <K, V> kafkaProducerChannel(
    producerProps: Map<String, Any>,
    producer: Producer<K, V> = KafkaProducer(producerProps)
): SendChannel<ProducerRecord<K, V>> = KafkaProducerChannel(producer)

/**
 * Kafka [Producer] object implementing the [SendChannel] methods.
 *
 * Note: This object is thread and coroutine safe and can operate from either.
 *
 * @param producer The instantiated [Producer] to use to send to kafka.
 */
open class KafkaProducerChannel<K, V>(private val producer: Producer<K, V>) : SendChannel<ProducerRecord<K, V>> {
    private val log = KotlinLogging.logger {}

    private val closed = AtomicBoolean(false)

    @ExperimentalCoroutinesApi
    override val isClosedForSend: Boolean = closed.get()

    override val onSend: SelectClause2<ProducerRecord<K, V>, SendChannel<ProducerRecord<K, V>>>
        get() {
            val parent: SendChannel<ProducerRecord<K, V>> = this
            return object : SelectClause2<ProducerRecord<K, V>, SendChannel<ProducerRecord<K, V>>> {
                @InternalCoroutinesApi
                override fun <R> registerSelectClause2(
                    select: SelectInstance<R>,
                    param: ProducerRecord<K, V>,
                    block: suspend (SendChannel<ProducerRecord<K, V>>) -> R
                ) {
                    if (!select.isSelected) {
                        return
                    }

                    sendOne(param)
                    runBlocking {
                        block(parent)
                    }
                }
            }
        }

    override fun close(cause: Throwable?): Boolean {
        if (closed.get()) {
            return true
        }

        return runCatching {
            producer.close()
            closed.set(true)
            true
        }.getOrDefault(false)
    }

    @ExperimentalCoroutinesApi
    override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {
    }

    override suspend fun send(element: ProducerRecord<K, V>) {
        if (closed.get()) {
            throw RuntimeException("closed")
        }

        withContext(Dispatchers.IO) {
            sendOne(element)
        }
    }

    private fun sendOne(record: ProducerRecord<K, V>, timeout: Duration = DEFAULT_SEND_TIMEOUT) {
        val meta = producer.send(record).get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        log.debug { "sent ${meta.serializedValueSize()} bytes to ${meta.topic()}-${meta.partition()}@${meta.offset()}" }
    }

    @OptIn(InternalCoroutinesApi::class)
    override fun trySend(element: ProducerRecord<K, V>): ChannelResult<Unit> {
        return try {
            sendOne(element)
            ChannelResult.success(Unit)
        } catch (e: Throwable) {
            close(e)
            ChannelResult.failure()
        }
    }
}
