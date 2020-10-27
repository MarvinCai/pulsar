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
public class AckMessageRequest {
    String ackType;

    List<List<String>> messagePositions;
}
