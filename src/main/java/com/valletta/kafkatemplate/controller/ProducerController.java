package com.valletta.kafkatemplate.controller;

import com.valletta.kafkatemplate.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    ProducerService producerService;

    @Autowired
    ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/message")
    public void publishMessage(@RequestParam String msg) {
        producerService.pub(msg);
    }
}
