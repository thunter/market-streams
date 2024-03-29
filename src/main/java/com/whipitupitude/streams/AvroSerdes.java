package com.whipitupitude.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import com.whipitupitude.market.TradeAvro;
import com.whipitupitude.market.PositionAvro;
import com.whipitupitude.market.PriceAvro;
import com.whipitupitude.market.TradeOpportunityAvro;

public class AvroSerdes {
    public static Serde<TradeAvro> TradeAvro(String url, boolean isKey) {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", url);
        Serde<TradeAvro> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }

    public static Serde<PositionAvro> PositionAvro(String url, boolean isKey) {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", url);
        Serde<PositionAvro> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }

    public static Serde<TradeOpportunityAvro> TradeOpportunityAvro(String url, boolean isKey) {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", url);
        Serde<TradeOpportunityAvro> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }

    public static Serde<PriceAvro> PriceAvro(String url, boolean isKey) {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", url);
        Serde<PriceAvro> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }
}
