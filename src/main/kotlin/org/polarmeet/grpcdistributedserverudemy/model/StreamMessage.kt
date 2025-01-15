package org.polarmeet.grpcdistributedserverudemy.model

data class StreamMessage(
    val data: String,
    val timestamp: Long = System.currentTimeMillis(),
)
