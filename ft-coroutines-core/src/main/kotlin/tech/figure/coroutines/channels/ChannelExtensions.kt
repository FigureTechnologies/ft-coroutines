package tech.figure.coroutines.channels

import kotlinx.coroutines.channels.ReceiveChannel

fun <T> ReceiveChannel<T>.receiveQueued(n: Int = Int.MAX_VALUE): List<T> {
    val list = mutableListOf<T>()

    var item: T?
    do {
        if (list.size >= n) {
            return list
        }

        item = tryReceive().getOrNull()
        if (item != null) {
            list.add(item)
        }
    } while (item != null)
    return list
}
