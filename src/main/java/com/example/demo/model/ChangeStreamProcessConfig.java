package com.example.demo.model;


import org.bson.BsonTimestamp;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeStreamProcessConfig<T> {
    private BsonTimestamp startAt;
    private BsonTimestamp endAt;
    private int noOfChangeStream;
    private int changeStreamIndex;

}
