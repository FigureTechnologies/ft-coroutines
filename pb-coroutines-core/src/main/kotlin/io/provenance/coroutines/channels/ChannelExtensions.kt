package io.provenance.coroutines.channels

import kotlinx.coroutines.channels.ReceiveChannel

fun <T> ReceiveChannel<T>.receiveQueued(): List<T> {
    val list = mutableListOf<T>()

    var item: T?
    do {
        item = tryReceive().getOrNull()
        if (item != null) {
            list.add(item)
        }
    } while (item != null)
    return list
}
