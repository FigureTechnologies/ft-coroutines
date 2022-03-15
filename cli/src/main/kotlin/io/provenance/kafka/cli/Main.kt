package io.provenance.kafka.cli

import ch.qos.logback.classic.Level
import io.provenance.kafka.coroutine.acking
import io.provenance.kafka.coroutine.kafkaChannel
import io.provenance.kafka.coroutine.onEachToTopic
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun logger(name: String): Logger = LoggerFactory.getLogger(name)
var Logger.level: Level
    get() = (this as ch.qos.logback.classic.Logger).level
    set(value) {
        (this as ch.qos.logback.classic.Logger).level = value
    }

@OptIn(ObsoleteCoroutinesApi::class)
fun main(args: Array<String>) {
    val parser = ArgParser("kafka-coroutines-copy-topic")
    val source by parser.option(ArgType.String, "source").required()
    val dest by parser.option(ArgType.String, "dest").required()
    val group by parser.option(ArgType.String, "group").required()
    val broker by parser.option(ArgType.String, "broker").required()
    parser.parse(args)

    val commonProps = mapOf<String, Any>(
        CommonClientConfigs.GROUP_ID_CONFIG to group,
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to broker,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
    )

    logger("org.apache.kafka").level = Level.WARN

    val incoming = kafkaChannel<String, String>(commonProps, setOf(source))
        .receiveAsFlow()
        .buffer()

    runBlocking {
        launch(Dispatchers.IO) {
            incoming.onEachToTopic(commonProps) { ProducerRecord(dest, it.key(), it.value()) }
                .buffer()
                .acking {
                    logger("main").info("acking record: ${it.key()} // ${it.value()}")
                }
                .collect()
        }

        launch(Dispatchers.IO) {
            val i = AtomicInteger(0)
            val producer = KafkaProducer<String, String>(commonProps)
            ticker(5000).receiveAsFlow().map {
                logger("main").info("ticker")
                producer.send(ProducerRecord(source, "test", "test-${i.getAndIncrement()}")).get()
            }.collect()
        }
    }
}
