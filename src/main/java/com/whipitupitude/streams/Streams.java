package com.whipitupitude.streams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.whipitupitude.market.PositionAvro;
import com.whipitupitude.market.PriceAvro;
import com.whipitupitude.market.TradeAvro;
import com.whipitupitude.market.TradeOpportunityAvro;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

public class Streams implements Callable<Integer> {

    @Option(names = "--kafka.properties", description = "Path to kafka.properties files", defaultValue = "kafka.properties")
    private String kafkaConfig = "kafka.properties";

    private static final Logger logger = LoggerFactory.getLogger(Streams.class);

    static final TopicNameExtractor<String, TradeAvro> buySellTopicExtractor = (key, trade, recordContext) -> {
        final String buySell = trade.getBuySell().toString();

        if (buySell.equals("B")) {
            return "trades.stream.buy";
        } else {
            return "trades.stream.sell";
        }
    };

    static Topology buildTopology(String inputTopic, String outputTopic, Properties properties) {

        //
        // Configure all the Serde (Serializers/Deserializers)
        // Note that the schema.registry.url is passed in.
        //
        Serde<String> stringSerde = Serdes.String();
        Serde<TradeAvro> tradeSerde = AvroSerdes.TradeAvro(properties.getProperty("schema.registry.url"), false);
        Serde<PositionAvro> positionSerde = AvroSerdes.PositionAvro(properties.getProperty("schema.registry.url"), false);
        Serde<TradeOpportunityAvro> opportunitySerde = AvroSerdes.TradeOpportunityAvro(properties.getProperty("schema.registry.url"), false);
        Serde<PriceAvro> priceSerde = AvroSerdes.PriceAvro(properties.getProperty("schema.registry.url"), false);

        // Create a new stream builder
        StreamsBuilder builder = new StreamsBuilder();

        // Create the stream which reads the 'trades' topic
        KStream<String, TradeAvro> tradesStream = builder.stream(inputTopic, Consumed.with(stringSerde, tradeSerde));

        // Filter the stream into buy/sell trades - the topic name extractor is a function which dynamically returns 
        // a topic to push the message into.
        tradesStream.to(buySellTopicExtractor, Produced.with(stringSerde, tradeSerde));

        //
        // Create a GlobalKTable - an abstraction, where the key is a key in a key-value store
        //
        GlobalKTable<String, PositionAvro> positionsTable = builder.globalTable("positions",
                Consumed.with(stringSerde, positionSerde));

        // Create a new stream consisting of
        // - the stream of trades
        // - the KTable of positions
        // Return an opportunity which has data combined from both.
        KStream<String, TradeOpportunityAvro> joinedStream = tradesStream.join(positionsTable,
                (leftKey, leftValue) -> leftKey, (trade, position) -> {
                    return new TradeOpportunityAvro(trade.getSymbol(), position.getLastTradePrice(), trade.getPrice(),
                            trade.getBuySell(), position.getPosition());
                });

        // Write the new stream out to a topic.
        joinedStream.to("trades.stream.opportunities", Produced.with(stringSerde, opportunitySerde));

        // // /* Start Price table logic */
        // KStream<String, PriceAvro> priceStream = tradesStream.mapValues((trade) -> {
        //     return new PriceAvro(trade.getSymbol(), trade.getPrice());
        // });

        // KTable<String, PriceAvro> priceTable = tradesStream.map((key, trade) -> new KeyValue<String, PriceAvro>(key,
        //         new PriceAvro(trade.getSymbol(), trade.getPrice()))).toTable(Materialized.as("prices-table"));

        // priceTable.toStream().to("prices", Produced.with(stringSerde, priceSerde));

        return builder.build();
    }

    public Integer call() throws Exception {

        Properties properties = new Properties();
        try {
            if (!Files.exists(Paths.get(kafkaConfig))) {
                throw new IOException(kafkaConfig + " not found");
            } else {
                try (InputStream inputStream = new FileInputStream(kafkaConfig)) {
                    properties.load(inputStream);
                }
            }
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        } catch (Exception e) {
            logger.error("Cannot configure Kafka " + kafkaConfig);
            throw new RuntimeException(e);
        }

        String inputTopic = "trades";
        String outputTopic = "buy_sells";

        //
        // Base idea of Streams is that it builds a topology, a DAG of Sources, Stream Processors and Sinks
        //
        Topology topology = buildTopology(inputTopic, outputTopic, properties);

        logger.info("Topology Created: " + topology);
        logger.info(topology.describe().toString());
        logger.info("Properties: " + properties);

        // 
        // Client that executes the defined topology.
        //
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        logger.info("Kafka Streams 101 App Started");
        runKafkaStreams(kafkaStreams);

        return 0;

    }
    
    static void runKafkaStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        //
        // Start execution of the stream and messages start processing.
        //
        streams.start();

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info("Streams Closed");
    }


    public static void main(String... args) {
        int exitCode = new CommandLine(new Streams()).execute(args);
        System.exit(exitCode);
    }
}


/*
 * 
 * 09:41:00.948 [main] INFO com.whipitupitude.streams.Streams - Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [trades])
      --> KSTREAM-LEFTJOIN-0000000005, KSTREAM-MAP-0000000008, KSTREAM-MAPVALUES-0000000007, KSTREAM-SINK-0000000001
    Processor: KSTREAM-MAP-0000000008 (stores: [])
      --> KSTREAM-FILTER-0000000011
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-FILTER-0000000011 (stores: [])
      --> KSTREAM-SINK-0000000010
      <-- KSTREAM-MAP-0000000008
    Processor: KSTREAM-LEFTJOIN-0000000005 (stores: [])
      --> KSTREAM-SINK-0000000006
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-MAPVALUES-0000000007 (stores: [])
      --> none
      <-- KSTREAM-SOURCE-0000000000
    Sink: KSTREAM-SINK-0000000001 (extractor class: com.whipitupitude.streams.Streams$$Lambda$4/0x0000000800c143a0@70ed52de)
      <-- KSTREAM-SOURCE-0000000000
    Sink: KSTREAM-SINK-0000000006 (topic: trades.stream.opportunities)
      <-- KSTREAM-LEFTJOIN-0000000005
    Sink: KSTREAM-SINK-0000000010 (topic: KSTREAM-TOTABLE-0000000009-repartition)
      <-- KSTREAM-FILTER-0000000011

  Sub-topology: 1 for global store (will not generate tasks)
    Source: KSTREAM-SOURCE-0000000003 (topics: [positions])
      --> KTABLE-SOURCE-0000000004
    Processor: KTABLE-SOURCE-0000000004 (stores: [positions-STATE-STORE-0000000002])
      --> none
      <-- KSTREAM-SOURCE-0000000003
  Sub-topology: 2
    Source: KSTREAM-SOURCE-0000000012 (topics: [KSTREAM-TOTABLE-0000000009-repartition])
      --> KSTREAM-TOTABLE-0000000009
    Processor: KSTREAM-TOTABLE-0000000009 (stores: [prices-table])
      --> KTABLE-TOSTREAM-0000000013
      <-- KSTREAM-SOURCE-0000000012
    Processor: KTABLE-TOSTREAM-0000000013 (stores: [])
      --> KSTREAM-SINK-0000000014
      <-- KSTREAM-TOTABLE-0000000009
    Sink: KSTREAM-SINK-0000000014 (topic: prices)
      <-- KTABLE-TOSTREAM-0000000013
 */