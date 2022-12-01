package tech.figure.kafka.coroutines.channels

import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.time.Duration
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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import tech.figure.kafka.loggingConsumerRebalanceListener
import tech.figure.kafka.records.CommitConsumerRecord
import tech.figure.kafka.records.UnAckedConsumerRecordImpl
import tech.figure.kafka.records.UnAckedConsumerRecords

internal fun <K, V> List<ConsumerRecord<K, V>>.toConsumerRecords() =
    groupBy { TopicPartition(it.topic(), it.partition()) }.let(::ConsumerRecords)

/**
 * Default is to create a committable consumer channel for unacknowledged record processing.
 *
 * @see [kafkaAckConsumerChannel]
 */
fun <K, V> kafkaConsumerChannel(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String = "kafka-channel",
    pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    rebalanceListener: ConsumerRebalanceListener = loggingConsumerRebalanceListener(),
    init: Consumer<K, V>.() -> Unit = { subscribe(topics, rebalanceListener) },
): ReceiveChannel<UnAckedConsumerRecords<K, V>> = kafkaAckConsumerChannel(consumerProperties, topics, name, pollInterval, consumer, rebalanceListener, init)

/**
 * Create a [ReceiveChannel] for [ConsumerRecords] from kafka.
 *
 * @param consumerProperties Kafka consumer settings for this channel.
 * @param topics Topics to subscribe to. Can be overridden via custom `init` parameter.
 * @param name The thread pool's base name for this consumer.
 * @param pollInterval Interval for kafka consumer [Consumer.poll] method calls.
 * @param consumer The instantiated [Consumer] to use to receive from kafka.
 * @param init Callback for initializing the [Consumer].
 * @return A non-running [KafkaConsumerChannel] instance that must be started via
 * [KafkaConsumerChannel.start].

 */
fun <K, V> kafkaNoAckConsumerChannel(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String = "kafka-channel",
    pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    rebalanceListener: ConsumerRebalanceListener = loggingConsumerRebalanceListener(),
    init: Consumer<K, V>.() -> Unit = { subscribe(topics, rebalanceListener) },
): ReceiveChannel<ConsumerRecords<K, V>> {
    return object :
        KafkaConsumerChannel<K, V, ConsumerRecords<K, V>>(
            consumerProperties,
            topics,
            name,
            pollInterval,
            consumer,
            init
        ) {
        override suspend fun preProcessPollSet(
            records: ConsumerRecords<K, V>,
            context: MutableMap<String, Any>
        ): List<ConsumerRecords<K, V>> {
            return listOf(records)
        }
    }
}

/**
 * Create a [ReceiveChannel] for unacknowledged consumer records from kafka.
 *
 * @param consumerProperties Kafka consumer settings for this channel.
 * @param topics Topics to subscribe to. Can be overridden via custom `init` parameter.
 * @param name The thread pool's base name for this consumer.
 * @param pollInterval Interval for kafka consumer [Consumer.poll] method calls.
 * @param consumer The instantiated [Consumer] to use to receive from kafka.
 * @param init Callback for initializing the [Consumer].
 * @return A non-running [KafkaConsumerChannel] instance that must be started via
 * [KafkaConsumerChannel.start].
 */
fun <K, V> kafkaAckConsumerChannel(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String = "kafka-channel",
    pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    rebalanceListener: ConsumerRebalanceListener = loggingConsumerRebalanceListener(),
    init: Consumer<K, V>.() -> Unit = { subscribe(topics, rebalanceListener) },
): ReceiveChannel<UnAckedConsumerRecords<K, V>> {
    return KafkaAckConsumerChannel(
            consumerProperties,
            topics,
            name,
            pollInterval,
            consumer,
            init
        )
        .also { Runtime.getRuntime().addShutdownHook(Thread { it.cancel() }) }
}

/**
 * Acking kafka [Consumer] object implementing the [ReceiveChannel] methods.
 *
 * Note: Must operate in a bound thread context regardless of coroutine assignment due to internal
 * kafka threading limitations for poll fetches, acknowledgements, and sends.
 *
 * @param consumerProperties Kafka consumer settings for this channel.
 * @param topics Topics to subscribe to. Can be overridden via custom `init` parameter.
 * @param name The thread pool's base name for this consumer.
 * @param pollInterval Interval for kafka consumer [Consumer.poll] method calls.
 * @param consumer The instantiated [Consumer] to use to receive from kafka.
 * @param init Callback for initializing the [Consumer].
 */
internal class KafkaAckConsumerChannel<K, V>(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String,
    pollInterval: Duration,
    consumer: Consumer<K, V>,
    init: Consumer<K, V>.() -> Unit
) :
    KafkaConsumerChannel<K, V, UnAckedConsumerRecords<K, V>>(
        consumerProperties,
        topics,
        name,
        pollInterval,
        consumer,
        init
    ) {
    override suspend fun preProcessPollSet(
        records: ConsumerRecords<K, V>,
        context: MutableMap<String, Any>,
    ): List<UnAckedConsumerRecords<K, V>> {
        log.trace { "preProcessPollSet(${records.count()})" }
        val ackChannel =
            Channel<CommitConsumerRecord>(capacity = records.count()).also {
                context["ack-channel"] = it
            }
        val unackedRecords =
            records
                .groupBy { "${it.topic()}-${it.partition()}" }
                .map {
                    val timestamp = System.currentTimeMillis()
                    val records =
                        it.value.map { UnAckedConsumerRecordImpl(it, ackChannel, timestamp) }
                    UnAckedConsumerRecords(records)
                }
        return unackedRecords
    }

    @Suppress("unchecked_cast")
    override suspend fun postProcessPollSet(
        records: List<UnAckedConsumerRecords<K, V>>,
        context: Map<String, Any>
    ) {
        log.trace { "postProcessPollSet(records:${records.sumOf { it.count() } })" }
        val ackChannel = context["ack-channel"]!! as Channel<CommitConsumerRecord>
        for (rs in records) {
            if (rs.records.isNotEmpty()) {
                val count = AtomicInteger(rs.records.size)
                while (count.getAndDecrement() > 0) {
                    log.trace { "waiting for ${count.get()} commits" }
                    val it = ackChannel.receive()
                    log.trace { "sending to broker ack(${it.duration.toMillis()}ms):${it.asCommitable()}" }
                    commit(it)
                    log.trace { "acking the commit back to flow" }
                    it.commitAck.send(Unit)
                }
            }
        }
        ackChannel.close()
    }
}

/**
 * Base kafka [Consumer] object implementing the [ReceiveChannel] methods.
 *
 * Note: Must operate in a bound thread context regardless of coroutine assignment due to internal
 * kafka threading limitations for poll fetches, acknowledgements, and sends.
 *
 * @param consumerProperties Kafka consumer settings for this channel.
 * @param topics Topics to subscribe to. Can be overridden via custom `init` parameter.
 * @param name The thread pool's base name for this consumer.
 * @param pollInterval Interval for kafka consumer [Consumer.poll] method calls.
 * @param consumer The instantiated [Consumer] to use to receive from kafka.
 * @param init Callback for initializing the [Consumer].
 */
abstract class KafkaConsumerChannel<K, V, R>(
    consumerProperties: Map<String, Any>,
    topics: Set<String> = emptySet(),
    name: String = "kafka-channel",
    private val pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    private val consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    private val init: Consumer<K, V>.() -> Unit = { subscribe(topics) },
) : ReceiveChannel<R> {
    companion object {
        private val threadCounter = AtomicInteger(0)
    }

    protected val log = KotlinLogging.logger {}
    private val thread =
        thread(
            name = "$name-${threadCounter.getAndIncrement()}",
            block = { run() },
            isDaemon = true,
            start = false
        )
    val sendChannel = Channel<R>(Channel.UNLIMITED)

    private fun <K, V> Consumer<K, V>.poll(duration: Duration) = poll(duration.toJavaDuration())

    private fun <T, L : Iterable<T>> L.ifEmpty(block: () -> L): L =
        if (count() == 0) block() else this

    protected abstract suspend fun preProcessPollSet(
        records: ConsumerRecords<K, V>,
        context: MutableMap<String, Any>
    ): List<R>

    protected open suspend fun postProcessPollSet(records: List<R>, context: Map<String, Any>) {}

    protected fun commit(record: CommitConsumerRecord): OffsetAndMetadata {
        consumer.commitSync(record.asCommitable())
        return record.offsetAndMetadata
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun run() {
        consumer.init()

        log.info("starting thread for ${consumer.subscription()}")
        runBlocking {
            log.info("${coroutineContext.job} running consumer ${consumer.subscription()}")
            try {
                while (!sendChannel.isClosedForSend) {
                    log.trace("poll(topics:${consumer.subscription()}) ...")
                    val polled =
                        consumer.poll(Duration.ZERO).ifEmpty { consumer.poll(pollInterval) }
                    val polledCount = polled.count()
                    if (polledCount == 0) {
                        continue
                    }

                    log.trace("poll(topics:${consumer.subscription()}) got $polledCount records.")

                    // Group by topic-partition to guarantee ordering.
                    val records =
                        polled
                            .groupBy { "${it.topic()}-${it.partition()}" }
                            .values
                            .map { it.toConsumerRecords() }

                    // Convert to internal types.
                    val context = mutableMapOf<String, Any>()
                    val processSet = records.map { preProcessPollSet(it, context) }

                    // Send down the pipeline for processing
                    processSet
                        .onEach { it.map { sendChannel.send(it) } }
                        // Clean up any processing.
                        .map { postProcessPollSet(it, context) }
                }
            } finally {
                log.info("${coroutineContext.job} shutting down consumer thread")
                try {
                    sendChannel.cancel(CancellationException("consumer shut down"))
                    consumer.unsubscribe()
                    consumer.close()
                } catch (ex: Exception) {
                    log.debug {
                        "Consumer failed to be closed. It may have been closed from somewhere else."
                    }
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

    @ExperimentalCoroutinesApi override val isEmpty: Boolean = sendChannel.isEmpty
    override val onReceive: SelectClause1<R>
        get() {
            start()
            return sendChannel.onReceive
        }

    override val onReceiveCatching: SelectClause1<ChannelResult<R>>
        get() {
            start()
            return sendChannel.onReceiveCatching
        }

    @Deprecated(
        "Since 1.2.0, binary compatibility with versions <= 1.1.x",
        level = DeprecationLevel.HIDDEN
    )
    override fun cancel(cause: Throwable?): Boolean {
        cancel(CancellationException("cancel", cause))
        return true
    }

    override fun cancel(cause: CancellationException?) {
        consumer.wakeup()
        sendChannel.cancel(cause)
    }

    override fun iterator(): ChannelIterator<R> {
        start()
        return sendChannel.iterator()
    }

    override suspend fun receive(): R {
        start()
        return sendChannel.receive()
    }

    override suspend fun receiveCatching(): ChannelResult<R> {
        start()
        return sendChannel.receiveCatching()
    }

    override fun tryReceive(): ChannelResult<R> {
        start()
        return sendChannel.tryReceive()
    }
}
