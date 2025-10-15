
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Semaphore;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CrptApi {

    private final int maxTokens;
    private final long tokenRefillPeriodNanos;
    private final Semaphore tokens;
    private final ScheduledExecutorService refillScheduler;
    private final ReentrantLock stateLock = new ReentrantLock();
    private volatile boolean isShutdown = false;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit <= 0) {
            throw new IllegalArgumentException("requestLimit must be > 0");
        }
        this.maxTokens = requestLimit;

        long nanosInUnit = timeUnit.toNanos(1);
        long computedRefill = nanosInUnit / requestLimit;
        if (computedRefill <= 0) {
            this.tokenRefillPeriodNanos = 1;
        } else {
            this.tokenRefillPeriodNanos = computedRefill;
        }

        this.tokens = new Semaphore(requestLimit, true);
        this.refillScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "CrptApi-RateLimiter");
            return t;
        });

        refillScheduler.scheduleAtFixedRate(() -> refillOneTokenIfNeeded(),
                tokenRefillPeriodNanos, tokenRefillPeriodNanos, TimeUnit.NANOSECONDS);
    }

    public void createDocument(Document document, String signature) {
        ensureNotShutdown();
        acquireTokenBlocking();
        try {
            HttpClientApi.post(document, signature);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create document", e);
        }
    }

    private void acquireTokenBlocking() {
        try {
            tokens.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted while waiting for rate limit token", e);
        }
    }

    private void ensureNotShutdown() {
        stateLock.lock();
        try {
            if (isShutdown) {
                throw new IllegalStateException("CrptApi is shutdown");
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void refillOneTokenIfNeeded() {
        if (isShutdown) {
            return;
        }
        int available = tokens.availablePermits();
        if (available < maxTokens) {
            tokens.release(1);
        }
    }

    public void shutdown() {
        stateLock.lock();
        try {
            if (isShutdown) {
                return;
            }
            isShutdown = true;
        } finally {
            stateLock.unlock();
        }
        refillScheduler.shutdownNow();
    }

    public static class HttpClientApi {

        private static final java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
        private static final ObjectMapper objectMapper = new ObjectMapper();
        private static final String url = System.getProperty("crpt.api.url");
        private static final String token = System.getProperty("crpt.api.token");
        private static final String productGroup = System.getProperty("crpt.api.product_group", "milk");

        public static void post(Document document, String signature) throws Exception {
            String documentJson = objectMapper.writeValueAsString(document);
            String documentBase64 = java.util.Base64
                    .getEncoder()
                    .encodeToString(documentJson.getBytes());
            String signatureBase64 = java.util.Base64
                    .getEncoder()
                    .encodeToString(signature.getBytes());

            HashMap<String, String> requestBody = new HashMap<>();
            requestBody.put("document_format", "MANUAL");
            requestBody.put("product_document", documentBase64);
            requestBody.put("product_group", productGroup);
            requestBody.put("signature", signatureBase64);
            requestBody.put("type", "LP_INTRODUCE_GOODS");

            String requestUrl = url + "?pg=" + productGroup;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(requestUrl))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)))
                    .build();

            HttpResponse<String> response =
             client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("API error: " + response.statusCode());
            }
        }
    }

    public static class Document {
        public Description description;
        public String doc_id;
        public String doc_status;
        public String doc_type;
        public boolean importRequest;
        public String owner_inn;
        public String participant_inn;
        public String producer_inn;
        public String production_date;
        public String production_type;
        public List<Product> products;
        public String reg_date;
        public String reg_number;
    }

    public static class Description {
        public String participantInn;
    }

    public static class Product {
        public String certificate_document;
        public String certificate_document_date;
        public String certificate_document_number;
        public String owner_inn;
        public String producer_inn;
        public String production_date;
        public String tnved_code;
        public String uit_code;
        public String uitu_code;
    }
}
