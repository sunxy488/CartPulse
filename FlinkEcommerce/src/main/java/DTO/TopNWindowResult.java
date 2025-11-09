package DTO;

import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

@Data
public class TopNWindowResult {
    private String hourKey; // e.g., 20250101T13Z or 2025010113
    private List<Tuple2<String, Double>> topN; // (productId, amount)
    private long windowStartMs; // window start (event time)
}
