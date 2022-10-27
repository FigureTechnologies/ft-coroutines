package tech.figure.kafka.context

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import tech.figure.kafka.records.UnAckedConsumerRecords

fun <K, V> Flow<UnAckedConsumerRecords<K, V>>.withKafkaContext(): KafkaValueFlow<K, V, UnAckedConsumerRecords<K, V>> =
    KafkaValueFlow(map { UnAckedValue(it.records, it) })

fun <R> Flow<AckedValue<*, *, R>>.dropKafkaContext(): Flow<R> =
    map { it.data }
