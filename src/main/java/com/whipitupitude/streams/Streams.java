package com.whipitupitude.streams;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import javax.swing.text.Position;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import com.whipitupitude.market.*;

public class Streams implements Callable<Integer> {

    @Option(names = "--kafka.properties", description = "Path to kafka.properties files", defaultValue = "kafka.properties")
    private String kafkaConfig = "kafka.properties";

    private static final Logger logger = LoggerFactory.getLogger(Streams.class);

    static void runKafkaStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        streams.start();

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info("Streams Closed");
    }

    static Topology buildTopology(String inputTopic, String outputTopic, Properties properties) {
        Serde<String> stringSerde = Serdes.String();
        Serde<TradeAvro> tradeSerde = AvroSerdes.TradeAvro(properties.getProperty("schema.registry.url"), false);
        Serde<PositionAvro> positionSerde = AvroSerdes.PositionAvro(properties.getProperty("schema.registry.url"),
                false);
        Serde<TradeOpportunityAvro> opportunitySerde = AvroSerdes
                .TradeOpportunityAvro(properties.getProperty("schema.registry.url"), false);

        final TopicNameExtractor<String, TradeAvro> buySellTopicExtractor = (key, trade, recordContext) -> {
            final String buySell = trade.getBuySell().toString();

            if (buySell.equals("B")) {
                return "trades.stream.buy";
            } else {
                return "trades.stream.sell";
            }
        };

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, TradeAvro> tradesStream = builder.stream(inputTopic, Consumed.with(stringSerde, tradeSerde));
        tradesStream.to(buySellTopicExtractor, Produced.with(stringSerde, tradeSerde));

        GlobalKTable<String, PositionAvro> positionsTable = builder.globalTable("positions",
                Consumed.with(stringSerde, positionSerde));

        ValueJoiner<TradeAvro, PositionAvro, TradeOpportunityAvro> tradeOppJoiner = (trade,
                position) -> new TradeOpportunityAvro(trade.getSymbol(), position.getLastTradePrice(), trade.getPrice(),
                        trade.getBuySell(), position.getPosition());

        KStream<String, TradeOpportunityAvro> joinedStream = tradesStream.join(positionsTable,
                (leftKey, leftValue) -> leftKey, (trade, position) -> {
                    return new TradeOpportunityAvro(trade.getSymbol(), position.getLastTradePrice(), trade.getPrice(),
                            trade.getBuySell(), position.getPosition());
                });

        // joinedStream.to("trades.stream.opportunities", Produced.with(stringSerde,
        // opportunitySerde));

        return builder.build();
    }

    public Integer call() throws Exception {

        // if (args.length < 1) {
        // throw new IllegalArgumentException("This program takes one argument: the path
        // to a configuration file.");
        // }

        Properties properties = new Properties();
        try {
            if (!Files.exists(Paths.get(kafkaConfig))) {
                throw new IOException(kafkaConfig + " not found");
            } else {
                try (InputStream inputStream = new FileInputStream(kafkaConfig)) {
                    properties.load(inputStream);
                }
            }

        } catch (Exception e) {
            logger.error("Cannot configure Kafka " + kafkaConfig);
            throw new RuntimeException(e);
        }

        String inputTopic = "trades";
        String outputTopic = "buy_sells";

        Topology topology = buildTopology(inputTopic, outputTopic, properties);

        logger.info("Topology Created: " + topology);
        logger.info("Properties: " + properties);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        logger.info("Kafka Streams 101 App Started");
        runKafkaStreams(kafkaStreams);

        return 0;

    }
    // }

    public static void main(String... args) {
        int exitCode = new CommandLine(new Streams()).execute(args);
        System.exit(exitCode);
    }
}
