package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 *
 */
@Data
@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class ConsumeMessageResponse {
    List<ConsumeMessageResult> results;

    /**
     *
     */
    @Setter
    @Getter
    public static class ConsumeMessageResult {
        int partition;
        long ledgerId;
        long entryId;
        String key;
        String value;
        String properties;
        long eventTime;
    }
}
