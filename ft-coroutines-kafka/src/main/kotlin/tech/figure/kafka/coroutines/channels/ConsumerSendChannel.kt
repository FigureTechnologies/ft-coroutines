package tech.figure.kafka.coroutines.channels

import tech.figure.kafka.loggingConsumerRebalanceListener
import tech.figure.kafka.records.CommitConsumerRecord
import tech.figure.kafka.records.UnAckedConsumerRecordImpl
import tech.figure.kafka.records.UnAckedConsumerRecords
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.toJavaDuration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelIterator
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.SelectClause1
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer

/**
 * Create a [ReceiveChannel] for unacknowledged consumer records from kafka.
 *
 * @param consumerProperties Kafka consumer settings for this channel.
 * @param topics Topics to subscribe to. Can be overridden via custom `init` parameter.
 * @param name The thread pool's base name for this consumer.
 * @param pollInterval Interval for kafka consumer [Consumer.poll] method calls.
 * @param consumer The instantiated [Consumer] to use to receive from kafka.
 * @param init Callback for initializing the [Consumer].
 * @return A non-running [KafkaConsumerChannel] instance that must be started via [KafkaConsumerChannel.start].
 */
fun <K, V> kafkaConsumerChannel(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String = "kafka-channel",
    pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    rebalanceListener: ConsumerRebalanceListener = loggingConsumerRebalanceListener(),
    init: Consumer<K, V>.() -> Unit = { subscribe(topics, rebalanceListener) },
): ReceiveChannel<UnAckedConsumerRecords<K, V>> {
    return KafkaConsumerChannel(consumerProperties, topics, name, pollInterval, consumer, init).also {
        Runtime.getRuntime().addShutdownHook(
            Thread {
                it.cancel()
            }
        )
    }
}

/**
 * Kafka [Consumer] object implementing the [ReceiveChannel] methods.
 *
 * Note: Must operate in a bound thread context regardless of coroutine assignment due to internal kafka threading
 * limitations for poll fetches, acknowledgements, and sends.
 *
 * @param consumerProperties Kafka consumer settings for this channel.
 * @param topics Topics to subscribe to. Can be overridden via custom `init` parameter.
 * @param name The thread pool's base name for this consumer.
 * @param pollInterval Interval for kafka consumer [Consumer.poll] method calls.
 * @param consumer The instantiated [Consumer] to use to receive from kafka.
 * @param init Callback for initializing the [Consumer].
 */
open class KafkaConsumerChannel<K, V>(
    consumerProperties: Map<String, Any>,
    topics: Set<String> = emptySet(),
    name: String = "kafka-channel",
    private val pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    private val consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    private val init: Consumer<K, V>.() -> Unit = { subscribe(topics) },
) : ReceiveChannel<UnAckedConsumerRecords<K, V>> {
    companion object {
        private val threadCounter = AtomicInteger(0)
    }

    private val log = KotlinLogging.logger {}
    private val thread =
        thread(name = "$name-${threadCounter.getAndIncrement()}", block = { run() }, isDaemon = true, start = false)
    private val sendChannel = Channel<UnAckedConsumerRecords<K, V>>(Channel.UNLIMITED)

    private inline fun <T> Channel<T>.use(block: (Channel<T>) -> Unit) {
        try {
            block(this)
            close()
        } catch (e: Throwable) {
            close(e)
        }
    }

    @OptIn(ExperimentalTime::class)
    private fun <K, V> Consumer<K, V>.poll(duration: Duration) =
        poll(duration.toJavaDuration())

    private fun <T, L : Iterable<T>> L.ifEmpty(block: () -> L): L =
        if (count() == 0) block() else this

    @OptIn(ExperimentalCoroutinesApi::class, ExperimentalTime::class)
    fun run() {
        consumer.init()

        log.info("starting thread for ${consumer.subscription()}")
        runBlocking {
            log.info("${coroutineContext.job} running consumer ${consumer.subscription()}")
            try {
                while (!sendChannel.isClosedForSend) {
                    log.trace("poll(topics:${consumer.subscription()}) ...")
                    val polled = consumer.poll(Duration.ZERO).ifEmpty { consumer.poll(pollInterval) }
                    val polledCount = polled.count()
                    if (polledCount == 0) {
                        continue
                    }

                    log.trace("poll(topics:${consumer.subscription()}) got $polledCount records.")
                    Channel<CommitConsumerRecord>(capacity = polled.count()).use { ackChannel ->
                        for (it in polled.groupBy { "${it.topic()}-${it.partition()}" }) {
                            val timestamp = System.currentTimeMillis()
                            val records = it.value.map {
                                UnAckedConsumerRecordImpl(it, ackChannel, timestamp)
                            }
                            sendChannel.send(UnAckedConsumerRecords(records))
                        }

                        if (polledCount > 0) {
                            val count = AtomicInteger(polledCount)
                            while (count.getAndDecrement() > 0) {
                                val it = ackChannel.receive()
                                log.debug { "ack(${it.duration.toMillis()}ms):${it.asCommitable()}" }
                                consumer.commitSync(it.asCommitable())
                                it.commitAck.send(Unit)
                            }
                        }
                    }
                }
            } finally {
                log.info("${coroutineContext.job} shutting down consumer thread")
                try {
                    sendChannel.cancel(CancellationException("consumer shut down"))
                    consumer.unsubscribe()
                    consumer.close()
                } catch (ex: Exception) {
                    log.debug { "Consumer failed to be closed. It may have been closed from somewhere else." }
                }
            }
        }
    }

    fun start() {
        if (!thread.isAlive) {
            synchronized(thread) {
                if (!thread.isAlive) {
                    log.info("starting consumer thread")
                    thread.start()
                }
            }
        }
    }

    @ExperimentalCoroutinesApi
    override val isClosedForReceive: Boolean = sendChannel.isClosedForReceive

    @ExperimentalCoroutinesApi
    override val isEmpty: Boolean = sendChannel.isEmpty
    override val onReceive: SelectClause1<UnAckedConsumerRecords<K, V>> get() {
        start()
        return sendChannel.onReceive
    }

    override val onReceiveCatching: SelectClause1<ChannelResult<UnAckedConsumerRecords<K, V>>> get() {
        start()
        return sendChannel.onReceiveCatching
    }

    @Deprecated("Since 1.2.0, binary compatibility with versions <= 1.1.x", level = DeprecationLevel.HIDDEN)
    override fun cancel(cause: Throwable?): Boolean {
        cancel(CancellationException("cancel", cause))
        return true
    }

    override fun cancel(cause: CancellationException?) {
        consumer.wakeup()
        sendChannel.cancel(cause)
    }

    override fun iterator(): ChannelIterator<UnAckedConsumerRecords<K, V>> {
        start()
        return sendChannel.iterator()
    }

    override suspend fun receive(): UnAckedConsumerRecords<K, V> {
        start()
        return sendChannel.receive()
    }

    override suspend fun receiveCatching(): ChannelResult<UnAckedConsumerRecords<K, V>> {
        start()
        return sendChannel.receiveCatching()
    }

    override fun tryReceive(): ChannelResult<UnAckedConsumerRecords<K, V>> {
        start()
        return sendChannel.tryReceive()
    }
}
