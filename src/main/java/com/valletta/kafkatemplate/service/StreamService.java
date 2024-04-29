package com.valletta.kafkatemplate.service;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
//    public void buildPipeline(StreamsBuilder sb) {
    public void buildPipeline(StreamsBuilder sb) {

//        KStream<String, String> myStream = sb.stream("vallettaTopic", Consumed.with(STRING_SERDE, STRING_SERDE));
//        myStream.print(Printed.toSysOut());
//        myStream.filter((key, value) -> value.contains("rich")).to("richList");

        KStream<String, String> leftStream = sb.stream("leftTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE));
        leftStream.print(Printed.toSysOut());
        // key:value --> 1:leftValue
        KStream<String, String> rightStream = sb.stream("rightTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE));
        rightStream.print(Printed.toSysOut());
        // key:value --> 1:rightValue

        ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) -> {
            return "[StringJoiner]" + leftValue + "-" + rightValue;
        };

        ValueJoiner<String, String, String> stringOuterJoiner = (leftValue, rightValue) -> {
            return "[StringOuterJoiner]" + leftValue + "<" + rightValue;
        };

        KStream<String, String> joinedStream = leftStream.join(rightStream,
                stringJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));

//        joinedStream.print(Printed.toSysOut());

        KStream<String, String> outerJoinedStream = leftStream.outerJoin(rightStream,
                stringOuterJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));

        joinedStream.print(Printed.toSysOut());
        joinedStream.to("joinedMsg2");
        outerJoinedStream.to("joinedMsg");

        final Topology topology = sb.build();
    }
}
