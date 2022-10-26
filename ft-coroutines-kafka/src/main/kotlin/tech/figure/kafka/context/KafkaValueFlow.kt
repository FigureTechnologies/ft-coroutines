package tech.figure.kafka.context

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.transform

open class KafkaValueFlow<K, V, T>(
    private val innerFlow: Flow<UnAckedValue<K, V, T>>
) {
    suspend fun collect(collector: FlowCollector<T>) {
        innerFlow.map { it.data }.collect { collector.emit(it) }
    }

    fun onEach(block: suspend (T) -> Unit): KafkaValueFlow<K, V, T> =
        KafkaValueFlow(innerFlow.onEach { block(it.data) })

    fun <R> map(block: suspend (T) -> R): KafkaValueFlow<K, V, R> =
        KafkaValueFlow(innerFlow.map { UnAckedValue(it.records, block(it.data)) })

    fun <E> flatMap(block: suspend (T) -> Iterable<E>): KafkaValueFlow<K, V, E> =
        KafkaValueFlow(innerFlow.transform { block(it.data) })

    fun acking(): Flow<AckedValue<K, V, T>> =
        innerFlow.map { it.ack() }
}
