package io.provenance.kafka.cli

import ch.qos.logback.classic.Level
import io.provenance.kafka.coroutine.kafkaConsumerChannel
import io.provenance.kafka.coroutine.kafkaProducerChannel
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
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
        CommonClientConfigs.GROUP_ID_CONFIG to group + OffsetDateTime.now().minute,
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to broker,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
    )

    logger("org.apache.kafka").level = Level.WARN

    val incoming = kafkaConsumerChannel<String, String>(commonProps, setOf(source))
    val producer = kafkaProducerChannel<String, String>(commonProps)

    runBlocking {

        //
        // Using select
        //

        launch(Dispatchers.IO) {
            val ticker = ticker(5000)
            val i = AtomicInteger(100)

            while (true) {
                select<Unit> {
                    incoming.onReceive {
                        logger("main").info("pre-commit: ${it.key} // ${it.value} on ${it.topic}-${it.partition}@${it.offset}")

                        val rec = it
                        producer.onSend(ProducerRecord(dest, it.key, it.value)) {
                            val ack = rec.ack()
                            logger("main").info("post-commit: ${ack.key} // ${ack.value} @ ${rec.offset}")
                        }
                    }

                    ticker.onReceive {
                        logger("main").info("ticker")
                        producer.send(ProducerRecord(source, dest, "test-${i.getAndIncrement()}"))
                    }
                }
            }
        }

        //
        // Using flows
        //

        launch(Dispatchers.IO) {
            incoming.receiveAsFlow().buffer().onEach {
                logger("main").info("pre-commit: ${it.key} // ${it.value} on ${it.topic}-${it.partition}@${it.offset}")
                producer.send(ProducerRecord(dest, it.key, it.value))
                val ack = it.ack()
                logger("main").info("post-commit: ${ack.key} // ${ack.value} @ ${it.offset}")
            }.collect()
        }

        launch(Dispatchers.IO) {
            val ticker = ticker(5000)
            val i = AtomicInteger(0)
            ticker.receiveAsFlow().onEach {
                logger("main").info("ticker")
                producer.send(ProducerRecord(source, dest, "test-${i.getAndIncrement()}"))
            }.collect()
        }
    }
}
