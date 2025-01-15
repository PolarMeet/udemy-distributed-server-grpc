package org.polarmeet.grpcdistributedserverudemy.controller

import org.polarmeet.grpcdistributedserverudemy.service.MessageBroker
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

// Publish messages to redis manually.  (Testing purpose)
// By default we are expecting out grpc server to get those messages in real time from some grpc client.

// http://localhost:8080/api/messages/publish -> make a post request

@RestController
@RequestMapping("/api/messages")
class MessageController( private val messageBroker: MessageBroker) {

   @PostMapping("/publish")
   suspend fun publishMessage(@RequestBody message: String): ResponseEntity<String> {
       messageBroker.publishMessage(message = message)
       return ResponseEntity.ok("Message published successfully")
   }
}