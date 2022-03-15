package io.provenance.kafka.coroutine

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.selects.SelectClause2
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

fun <K, V> kafkaProducerChannel(
	producerProps: Map<String, Any>,
	producer: Producer<K, V> = KafkaProducer(producerProps)
): SendChannel<ProducerRecord<K, V>> = KafkaProducerChannel(producer)

@OptIn(ExperimentalTime::class)
val DEFAULT_SEND_TIMEOUT = 10.seconds

open class KafkaProducerChannel<K, V>(private val producer: Producer<K, V>) : SendChannel<ProducerRecord<K, V>> {
	private val log = KotlinLogging.logger {}

	private val closed = AtomicBoolean(false)

	@ExperimentalCoroutinesApi
	override val isClosedForSend: Boolean = closed.get()

	override val onSend: SelectClause2<ProducerRecord<K, V>, SendChannel<ProducerRecord<K, V>>>
		get() { TODO() }

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
	override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {}

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
		log.info("successfully sent ${meta.serializedValueSize()} ${meta.topic()}-${meta.partition()}@${meta.offset()}")
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