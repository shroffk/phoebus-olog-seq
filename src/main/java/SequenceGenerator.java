import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

public class SequenceGenerator {

    private static final String OLOG_SEQ = "olog_seq";
    private static final OlogSequence seq = new OlogSequence();

    public static void main(String[] args) {
        // Create the low-level client
        RestClient httpClient = RestClient.builder(new HttpHost("localhost", 9228)).build();

        // Create the Java API Client with the same low level client
        ElasticsearchTransport transport = new RestClientTransport(
                httpClient,
                new JacksonJsonpMapper()
        );

        ElasticsearchClient client = new ElasticsearchClient(transport);

        try {
            BooleanResponse exits = client.indices().exists(ExistsRequest.of(e -> e.index(OLOG_SEQ)));
            System.out.println("found index " + OLOG_SEQ + ":" + exits.value());

            if(!exits.value()) {
                InputStream is = SequenceGenerator.class.getResourceAsStream("/seq_mapping.json");
                CreateIndexResponse result = client.indices().create(
                        CreateIndexRequest.of(
                                c -> c.index(OLOG_SEQ)
                                        .withJson(is)));

                System.out.println( "Created " + OLOG_SEQ + " index." + result.toString());
            }


            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            IndexRequest request = IndexRequest.of(i -> i.index(OLOG_SEQ)
                    .document(JsonData.of(seq, new JacksonJsonpMapper(objectMapper)))
                    .refresh(Refresh.True));

            IndexResponse response = client.index(request);
            System.out.println("response " + response.seqNo());
            response = client.index(request);
            System.out.println("response " + response.seqNo());
            response = client.index(request);
            System.out.println("response " + response.seqNo());
            response = client.index(request);
            System.out.println("response " + response.seqNo());

            ExecutorService service = Executors.newFixedThreadPool(5);
            for (int i = 0; i < 50; i++) {
                service.submit(() -> {
                    try {
                        IndexResponse r = client.index(request);
                        System.out.println("Thread" + Thread.currentThread().getName() + " response " + r.seqNo());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            service.shutdown();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class OlogSequence {
        private final Instant createDate;

        OlogSequence() {
            createDate = Instant.now();
        }

        public Instant getCreateDate() {
            return createDate;
        }
    }
}
