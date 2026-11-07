import java.util.LinkedHashMap;
import java.util.Optional;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;

/** Isolated client-state reproduction, not a gateway or network conformance test. */
public final class FetchCloseReproduction {
    private static final TopicPartition PARTITION = new TopicPartition("test-topic", 0);
    private static final short FETCH_VERSION = 4;

    private static FetchSessionHandler pendingFetch() {
        FetchSessionHandler handler = new FetchSessionHandler(new LogContext(), 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(PARTITION, new FetchRequest.PartitionData(
                Uuid.ZERO_UUID, 0, -1, 1048576, Optional.empty()));
        builder.build();
        if (!handler.sessionTopicPartitions().contains(PARTITION))
            throw new AssertionError("request must contain the partition");
        return handler;
    }

    private static FetchResponse response() {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> partitions =
                new LinkedHashMap<>();
        partitions.put(new TopicIdPartition(Uuid.ZERO_UUID, PARTITION),
                FetchResponse.partitionResponse(0, Errors.NONE));
        return FetchResponse.of(Errors.NONE, 0, 0, partitions);
    }

    public static void main(String[] args) {
        FetchSessionHandler normal = pendingFetch();
        if (!normal.handleResponse(response(), FETCH_VERSION))
            throw new AssertionError("ordinary full response must be accepted");

        FetchSessionHandler closing = pendingFetch();
        // Same ordering as AbstractFetch.maybeCloseFetchSessions in Kafka 3.6.1:
        // notify, build an empty close request, then poll outstanding responses.
        closing.notifyClose();
        closing.newBuilder().build();
        if (!closing.sessionTopicPartitions().isEmpty())
            throw new AssertionError("empty close request must replace partition set");
        if (closing.handleResponse(response(), FETCH_VERSION))
            throw new AssertionError("late original response must be rejected");

        FetchSessionHandler drained = pendingFetch();
        drained.notifyClose();
        if (!drained.handleResponse(response(), FETCH_VERSION))
            throw new AssertionError("notifyClose alone must not reject original response");
        System.out.println("PASS: normal response accepted; empty closing build makes the same pending response invalid; notifyClose alone accepts it.");
        System.out.println("Scope: pinned client state only; does not prove observed gateway response correctness or absence of record loss.");
    }
}
