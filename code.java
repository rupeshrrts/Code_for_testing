import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.ListUsersRequest;
import software.amazon.awssdk.services.iam.model.ListUsersResponse;

/**
 * This program fetches user data from three identity providers (AWS IAM, Google Workspace, and Azure Entra ID)
 * and ingests the data into a Neo4j database. It demonstrates secure programming practices and modular design.
 */
public class IdentityToNeo4j {

    // API URLs for Google Workspace and Azure Entra ID
    private static final String GOOGLE_WORKSPACE_API = "https://admin.googleapis.com/admin/directory/v1/users";
    private static final String AZURE_ENTRA_API = "https://graph.microsoft.com/v1.0/users";

    // Environment variables for Neo4j credentials and connection
    private static final String NEO4J_URI = System.getenv("NEO4J_URI");
    private static final String NEO4J_USER = System.getenv("NEO4J_USER");
    private static final String NEO4J_PASSWORD = System.getenv("NEO4J_PASSWORD");

    // Shared HTTP client and JSON parser
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        try (Driver driver = GraphDatabase.driver(NEO4J_URI, AuthTokens.basic(NEO4J_USER, NEO4J_PASSWORD));
             Session session = driver.session()) {

            // Fetch and ingest users from AWS Identity
            List<JsonNode> awsUsers = fetchUsersFromAWSIdentity(System.getenv("AWS_ACCESS_KEY"), System.getenv("AWS_SECRET_KEY"));
            ingestUsersToNeo4j(session, awsUsers, "AWSIdentity");

            // Fetch and ingest users from Google Workspace
            List<JsonNode> googleUsers = fetchUsersFromGoogleWorkspace(System.getenv("GOOGLE_API_KEY"));
            ingestUsersToNeo4j(session, googleUsers, "GoogleWorkspace");

            // Fetch and ingest users from Azure Entra ID
            List<JsonNode> azureUsers = fetchUsersFromAzureEntraID(System.getenv("AZURE_ACCESS_TOKEN"));
            ingestUsersToNeo4j(session, azureUsers, "AzureEntra");

        } catch (Exception e) {
            System.err.println("An error occurred while processing identity data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Fetches a list of users from AWS Identity and Access Management (IAM).
     * @param accessKey AWS Access Key
     * @param secretKey AWS Secret Key
     * @return List of users as JSON nodes
     */
    private static List<JsonNode> fetchUsersFromAWSIdentity(String accessKey, String secretKey) {
        IamClient iam = IamClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

        ListUsersRequest request = ListUsersRequest.builder().build();
        ListUsersResponse response = iam.listUsers(request);

        return response.users().stream()
                .map(user -> objectMapper.createObjectNode().put("userName", user.userName()))
                .toList();
    }

    /**
     * Fetches a list of users from Google Workspace using the Admin SDK API.
     * @param apiKey Google API Key
     * @return List of users as JSON nodes
     * @throws IOException, InterruptedException
     */
    private static List<JsonNode> fetchUsersFromGoogleWorkspace(String apiKey) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(GOOGLE_WORKSPACE_API + "?key=" + apiKey))
                .header("Authorization", "Bearer " + apiKey)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode rootNode = objectMapper.readTree(response.body());
        return rootNode.get("users").findValues("primaryEmail");
    }

    /**
     * Fetches a list of users from Azure Entra ID using the Microsoft Graph API.
     * @param accessToken Azure OAuth Access Token
     * @return List of users as JSON nodes
     * @throws IOException, InterruptedException
     */
    private static List<JsonNode> fetchUsersFromAzureEntraID(String accessToken) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(AZURE_ENTRA_API))
                .header("Authorization", "Bearer " + accessToken)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode rootNode = objectMapper.readTree(response.body());
        return rootNode.get("value").findValues("userPrincipalName");
    }

    /**
     * Ingests a list of users into Neo4j database under the specified source.
     * @param session Neo4j database session
     * @param users List of users as JSON nodes
     * @param source Source identifier (e.g., "AWSIdentity")
     */
    private static void ingestUsersToNeo4j(Session session, List<JsonNode> users, String source) {
        for (JsonNode user : users) {
            String identifier = user.has("userName") ? user.get("userName").asText() : user.asText();
            session.writeTransaction(tx -> {
                tx.run("MERGE (u:User {identifier: $identifier}) SET u.source = $source",
                        org.neo4j.driver.Values.parameters("identifier", identifier, "source", source));
                return null;
            });
        }
    }
}