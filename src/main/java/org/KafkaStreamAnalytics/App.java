package org.KafkaStreamAnalytics;

import org.KafkaStreamAnalytics.Models.AssetAggregation;
import org.KafkaStreamAnalytics.Models.Quote;
import org.KafkaStreamAnalytics.Serializers.JsonDeserializer;
import org.KafkaStreamAnalytics.Serializers.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;

public class App {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamingConfig = new StreamsConfig(props);

        // serde setup
        Serde<String> stringSerde = Serdes.String();
        JsonDeserializer<Quote> quoteJsonDeserializer = new JsonDeserializer<>(Quote.class);
        JsonSerializer<Quote> quoteJsonSerializer = new JsonSerializer<>();

        Serde<Quote> quoteSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Quote.class));

        Serde<AssetAggregation> assetAggregationSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(AssetAggregation.class));

        StreamsBuilder builder = new StreamsBuilder();

        // created windowed table
        long twentySeconds = 1000 * 20;
        long fifteenMinutes = 1000 * 60 * 15;

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("quotes-store");

        KStream<String, Quote> stream = builder.stream("quotes", Consumed.with(stringSerde, quoteSerde));

        KGroupedStream<String, Quote> groupedStream = stream.groupByKey(Serialized.with(stringSerde, quoteSerde));

        TimeWindowedKStream<String, Quote> windowedStream = groupedStream.windowedBy(TimeWindows.of(twentySeconds).until(fifteenMinutes));

        KTable<Windowed<String>, AssetAggregation> timeWindowedAggregatedStream = windowedStream
                .aggregate(
                        AssetAggregation::new, /* initializer */
                        (aggKey, newValue, aggValue) -> aggValue.addValue(newValue, aggKey), /* adder */
                        Materialized.<String, AssetAggregation, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store") /* state store name */
                                .withValueSerde(assetAggregationSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        KStream<Windowed<String>, AssetAggregation> outputStream = timeWindowedAggregatedStream
                .toStream();

        outputStream
                .map((Windowed<String> key, AssetAggregation value) -> new KeyValue(key.key(), value))
                .to("quotes_out", Produced.with(Serdes.String(), assetAggregationSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamingConfig);

        kafkaStreams.start();

    }
}