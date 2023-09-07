package com.example.demo.model;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.mongodb.core.mapping.Field;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Name {
    String en;
    
    @BsonProperty("zh-hk")
    @JsonProperty("zh-hk")
    @Field("zh-hk")
    String zhhk;

    @BsonProperty("zh-cn")
    @JsonProperty("zh-cn")
    @Field("zh-cn")
    String zhcn; 
}
