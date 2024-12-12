package org.polarmeet.grpcdistributedserverudemy.controller

import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactor.asFlux
import org.polarmeet.grpcdistributedserverudemy.service.MessageBroker
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux

// Here we will be defining the route for SSE, so the users can listen for real time updates on this URL
// we need to put a stream of messages to end user.

@RestController
@RequestMapping("/sse")
class SSEController(private val messageBroker: MessageBroker) {
    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
   fun streamMessages(): Flux<String> {
       return messageBroker.messageFlow
           .map { message ->
               """
               data: $message    
               
               """.trimIndent()
           }
           .asFlux()
   }
}