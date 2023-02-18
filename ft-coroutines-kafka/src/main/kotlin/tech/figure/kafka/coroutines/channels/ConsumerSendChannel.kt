package tech.figure.kafka.coroutines.channels

import java.util.concurrent.CountDownLatch
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
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import tech.figure.kafka.loggingConsumerRebalanceListener
import tech.figure.kafka.records.CommitConsumerRecord
import tech.figure.kafka.records.UnAckedConsumerRecord
import tech.figure.kafka.records.UnAckedConsumerRecordImpl

private const val DEFAULT_MAX_POLL_RECORDS = 500
private const val DEFAULT_BUFFER_FACTOR = 3

private val Map<String, Any>.maxPollBufferCapacity
    get(): Int {
        // consumerConfig defined max poll records.
        val fromConfig = this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] as? Int
        // fallback to kafka default.
        val fromKafkaDefault =
            ConsumerConfig.configDef().defaultValues()[ConsumerConfig.MAX_POLL_RECORDS_CONFIG]
                as? Int
        // when all else fails...
        val failsafe = DEFAULT_MAX_POLL_RECORDS

        val maxPollRecords = fromConfig ?: fromKafkaDefault ?: failsafe
        return maxPollRecords * DEFAULT_BUFFER_FACTOR
    }

/**
 * Default is to create a committable consumer channel for unacknowledged record processing.
 *
 * @see [kafkaAckConsumerChannel]
 */
fun <K, V> kafkaConsumerChannel(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String = "kafka-channel",
    bufferCapacity: Int = consumerProperties.maxPollBufferCapacity,
    pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    rebalanceListener: ConsumerRebalanceListener = loggingConsumerRebalanceListener(),
    init: Consumer<K, V>.() -> Unit = { subscribe(topics, rebalanceListener) },
): ReceiveChannel<List<UnAckedConsumerRecord<K, V>>> =
    kafkaAckConsumerChannel(
        consumerProperties,
        topics,
        name,
        bufferCapacity,
        pollInterval,
        consumer,
        rebalanceListener,
        init
    )

private fun <K, V> noAckConsumerInit(
    topics: Set<String>,
    seekTopicPartitions: Consumer<K, V>.(List<TopicPartition>) -> Unit
): (Consumer<K, V>) -> Unit = { consumer ->
    val tps =
        topics.flatMap {
            consumer.partitionsFor(it).map { TopicPartition(it.topic(), it.partition()) }
        }
    consumer.assign(tps)
    consumer.seekTopicPartitions(tps)
}

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
 *   [KafkaConsumerChannel.start].
 */
fun <K, V> kafkaNoAckConsumerChannel(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String = "kafka-channel",
    bufferCapacity: Int = consumerProperties.maxPollBufferCapacity,
    pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    seekTopicPartitions: Consumer<K, V>.(List<TopicPartition>) -> Unit = {},
): ReceiveChannel<List<ConsumerRecord<K, V>>> {
    return object :
        KafkaConsumerChannel<K, V, ConsumerRecord<K, V>>(
            consumerProperties,
            topics,
            name,
            bufferCapacity,
            pollInterval,
            consumer,
            noAckConsumerInit(topics, seekTopicPartitions),
        ) {
        override suspend fun preProcessPollSet(
            topicPartition: TopicPartition,
            records: List<ConsumerRecord<K, V>>,
            context: MutableMap<String, Any>
        ): List<ConsumerRecord<K, V>> {
            return records
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
 *   [KafkaConsumerChannel.start].
 */
fun <K, V> kafkaAckConsumerChannel(
    consumerProperties: Map<String, Any>,
    topics: Set<String>,
    name: String = "kafka-channel",
    bufferCapacity: Int = consumerProperties.maxPollBufferCapacity,
    pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    rebalanceListener: ConsumerRebalanceListener = loggingConsumerRebalanceListener(),
    init: Consumer<K, V>.() -> Unit = { subscribe(topics, rebalanceListener) },
): ReceiveChannel<List<UnAckedConsumerRecord<K, V>>> {
    return KafkaAckConsumerChannel(
        consumerProperties,
        topics,
        name,
        bufferCapacity,
        pollInterval,
        consumer,
        init
    ).also { Runtime.getRuntime().addShutdownHook(Thread { it.cancel() }) }
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
    bufferCapacity: Int,
    pollInterval: Duration,
    consumer: Consumer<K, V>,
    init: Consumer<K, V>.() -> Unit
) :
    KafkaConsumerChannel<K, V, UnAckedConsumerRecord<K, V>>(
        consumerProperties,
        topics,
        name,
        bufferCapacity,
        pollInterval,
        consumer,
        init
    ) {
    override suspend fun preProcessPollSet(
        topicPartition: TopicPartition,
        records: List<ConsumerRecord<K, V>>,
        context: MutableMap<String, Any>,
    ): List<UnAckedConsumerRecord<K, V>> {
        log.trace { "preProcessPollSet(tp:$topicPartition count:${records.size})" }
        val ackChannel =
            Channel<CommitConsumerRecord>(capacity = records.size).also {
                context["ack-channel"] = it
            }
        return records.map {
            val timestamp = System.currentTimeMillis()
            UnAckedConsumerRecordImpl(it, ackChannel, timestamp)
        }
    }

    @Suppress("unchecked_cast")
    override suspend fun postProcessPollSet(
        topicPartition: TopicPartition,
        records: List<UnAckedConsumerRecord<K, V>>,
        context: Map<String, Any>
    ) {
        log.trace { "postProcessPollSet(tp:$topicPartition count:${records.size})" }
        val ackChannel = context["ack-channel"]!! as ReceiveChannel<CommitConsumerRecord>
        if (records.isEmpty()) {
            log.trace { "empty record set, not waiting for acks" }
            return
        }

        val latch = CountDownLatch(records.size)
        log.trace { "poll group needs ${latch.count} total acks" }
        do {
            log.trace { "waiting for ${latch.count} more acks" }
            val it = ackChannel.receive()
            latch.countDown()

            log.trace { "ack received: ${it.topicPartition} => ${it.offsetAndMetadata}" }
            log.trace {
                " -> sending to broker ack(${it.duration.toMillis()}ms):${it.asCommitable()}"
            }
            commit(it)

            log.trace { "acking the commit back to flow" }
            it.commitAck.send(Unit)
        } while (latch.count > 0)
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
    bufferCapacity: Int = consumerProperties.maxPollBufferCapacity,
    private val pollInterval: Duration = DEFAULT_POLL_INTERVAL,
    private val consumer: Consumer<K, V> = KafkaConsumer(consumerProperties),
    private val init: Consumer<K, V>.() -> Unit = { subscribe(topics) },
) : ReceiveChannel<List<R>> {
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
    private val sendChannel = Channel<List<R>>(capacity = bufferCapacity)
    private fun <K, V> Consumer<K, V>.poll(duration: Duration) = poll(duration.toJavaDuration())

    private fun <T, L : Iterable<T>> L.ifEmpty(block: () -> L): L =
        if (count() == 0) block() else this

    protected abstract suspend fun preProcessPollSet(
        topicPartition: TopicPartition,
        records: List<ConsumerRecord<K, V>>,
        context: MutableMap<String, Any>
    ): List<R>

    protected open suspend fun postProcessPollSet(
        topicPartition: TopicPartition,
        records: List<R>,
        context: Map<String, Any>
    ) {
        /* no-op */
    }

    protected fun commit(record: CommitConsumerRecord): OffsetAndMetadata {
        val committable = record.asCommitable()
        log.trace { "trying commit => $committable" }
        consumer.commitSync(committable)
        log.trace { "trying commit success! $committable" }
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

                    // Convert to internal types.
                    val context = mutableMapOf<String, Any>()
                    val polledPartitions = polled.partitions()
                    val preSet = polledPartitions.map { it to polled.records(it) }
                    val inflightSet =
                        preSet.map { (tp, records) ->
                            tp to preProcessPollSet(tp, records, context)
                        }

                    // Post-processing set.
                    inflightSet.onEach { (tp, records) ->
                        sendChannel.send(records)
                        tp to postProcessPollSet(tp, records, context)
                    }
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
    override val onReceive: SelectClause1<List<R>>
        get() {
            start()
            return sendChannel.onReceive
        }

    override val onReceiveCatching: SelectClause1<ChannelResult<List<R>>>
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

    override fun iterator(): ChannelIterator<List<R>> {
        start()
        return sendChannel.iterator()
    }

    override suspend fun receive(): List<R> {
        start()
        return sendChannel.receive()
    }

    override suspend fun receiveCatching(): ChannelResult<List<R>> {
        start()
        return sendChannel.receiveCatching()
    }

    override fun tryReceive(): ChannelResult<List<R>> {
        start()
        return sendChannel.tryReceive()
    }
}
