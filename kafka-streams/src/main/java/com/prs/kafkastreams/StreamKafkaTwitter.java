package com.prs.kafkastreams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonParser;


public class StreamKafkaTwitter {
    private static JsonParser jsonParser = new JsonParser();
    private static Logger logger = LoggerFactory.getLogger(StreamKafkaTwitter.class.getName());
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String,String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredTopic = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersCountInTweet(jsonTweet) > 10000
        );
        filteredTopic.to("important_tweets");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);
        kafkaStreams.start();
    }

    private static Integer extractUserFollowersCountInTweet(final String jsonTweet) {
        try{
            return jsonParser.parse(jsonTweet).getAsJsonObject().
                    get("user").getAsJsonObject().get("followers_count").getAsInt();
        }
        catch (NullPointerException ex){
            logger.warn("Bad data");
            return 0;
        }
    }
}
