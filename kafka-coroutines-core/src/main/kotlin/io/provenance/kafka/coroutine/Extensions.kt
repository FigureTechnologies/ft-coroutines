package io.provenance.kafka.coroutine

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.apache.kafka.clients.consumer.ConsumerRecord

internal fun <T, L : Iterable<T>> L.ifEmpty(block: () -> L): L = if (count() == 0) block() else this

fun <K, V> Flow<UnAckedConsumerRecord<K, V>>.acking(
	block: (ConsumerRecord<K, V>) -> Unit = {}
): Flow<AckedConsumerRecord<K, V>> = map {
	block(it.record)
	it.ack()
}