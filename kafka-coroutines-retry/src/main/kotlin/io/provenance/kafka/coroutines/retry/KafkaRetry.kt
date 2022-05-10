package io.provenance.kafka.coroutines.retry

import org.apache.kafka.clients.consumer.ConsumerRecord

interface KafkaRetry<K, V> : FlowRetry<ConsumerRecord<K, V>>