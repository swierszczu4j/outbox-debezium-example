package com.example.debezium.outbox

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class ExampleController(
        private val exampleService: ExampleService
) {

    @PostMapping("/examples")
    fun addExample(@RequestBody exampleEntity: ExampleEntity) {
        exampleService.addExample(exampleEntity)
    }

}
