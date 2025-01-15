package org.polarmeet.grpcdistributedserverudemy.service

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.listener.PatternTopic
import org.springframework.stereotype.Service

@Service
class MessageBroker(
    private val redisTemplate: ReactiveRedisTemplate<String, String>
) {
    // This is very important part.
   private val _messageFlow = MutableSharedFlow<String>(
       replay = 0,
       extraBufferCapacity = 100000
   )

   // immutable shared flow for messages
    val messageFlow = _messageFlow.asSharedFlow()

    companion object {
        const val CHANNEL = "realtime-messages"
    }

    init {
        redisTemplate.listenTo(PatternTopic(CHANNEL))
            .map { message -> message.message}
            .subscribe { message ->
                _messageFlow.tryEmit(message)
            }
    }

    suspend fun publishMessage(message: String) {
        redisTemplate.convertAndSend(CHANNEL, message).awaitSingle()
    }
}

// Think of it like a water system

// 1. gRPC server = water source (messages)
// 2. Redis - Main water pipeline (distributes across buildings)
// 3. SharedFlow - Building's internal pipe system (distributed to apartments)
// 4. SSE endpoints - individual taps (deliver to end users)

// Flow

// gRPC server -> Redis pubsub -> MessageBroker -> SharedFlow -> SSE Endpoints -> Users

// MutableSharedFlow -> it's always "hot" = it's always running and ready to receive / transmit
// it can have multiple subscribers
// SharedFlow is like a pipe system
// has one input which is from Redis
// can have many outputs using SSE (in this case)
// Never stops flowing
// automatically handles backpressure upto 100k in this case
// Distributes the same message to all connected clients