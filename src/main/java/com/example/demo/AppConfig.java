package com.example.demo;

import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@Configuration
@EnableRetry
public class AppConfig {
    //@Value("${spring.data.mongodb.uri}")
    //private String uri;

    //@Bean
    //public MongoClient mongoClient() {
    //    return MongoClients.create(uri);
        /*return MongoClients.create(
    MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
    .applyToConnectionPoolSettings(builder ->
        builder.maxWaitTime(10, SECONDS)
        .maxSize(200).build()));*/
    //}

    @Bean
    public CodecRegistry pojoCodecRegistry() {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();

        return CodecRegistries.fromRegistries(
        //CodecRegistries.fromCodecs(new DateAsStringCodec(MongoClientSettings.getDefaultCodecRegistry())),
        MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(pojoCodecProvider));
    }
}
