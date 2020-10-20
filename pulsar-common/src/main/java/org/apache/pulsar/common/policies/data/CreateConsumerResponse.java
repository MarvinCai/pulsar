package org.apache.pulsar.common.policies.data;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 *
 */
@Data
@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class CreateConsumerResponse {
    List<CreateConsumerResponse.CreateConsumerResult> results;

    String url;

    /**
     *
     */
    @Setter
    @Getter
    @AllArgsConstructor
    public static class CreateConsumerResult {
        String consumerId;

        int partitionNumber;
    }
}
