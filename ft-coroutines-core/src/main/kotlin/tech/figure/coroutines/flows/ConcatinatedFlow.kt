package tech.figure.coroutines.flows

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

fun main() = runBlocking {
    val a = flowOf(*(1..50).toList().toTypedArray()).map {
        delay(100)
        it
    }

    val ch = Channel<Int>(UNLIMITED)
    launch(Dispatchers.IO) {
        var i = 1001
        while (true) {
            delay(300)
            println("sending $i")
            ch.send(i++)
        }
    }

    flowOf(a, ch.receiveAsFlow()).flattenConcat().collect {
        println("item: $it")
    }
}

// missing
// 7002444
// 7012521
// 7012524
// 7012525
// 7012526
// 7012527
// 7012528
// 7012529
// 7012530
// 8141124