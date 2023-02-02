package tech.figure.kafka.coroutines.retry

import java.nio.ByteBuffer

fun Int.toByteArray() =
    ByteBuffer.allocate(Int.SIZE_BYTES).putInt(this).array()

fun ByteArray.toInt() =
    ByteBuffer.wrap(this).int
