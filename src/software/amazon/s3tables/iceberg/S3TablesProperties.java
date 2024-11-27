package software.amazon.s3tables.iceberg;

import software.amazon.s3tables.iceberg.S3TablesAwsClientFactories.DefaultS3TablesAwsClientFactory;
import software.amazon.awssdk.services.s3tables.S3TablesClientBuilder;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

public class S3TablesProperties implements Serializable {
    /**
     * This property is used to pass in the aws client factory implementation class for S3 Tables. The
     * class should implement {@link S3TablesAwsClientFactory}. For example, {@link
     * DefaultS3TablesAwsClientFactory} implements {@link S3TablesAwsClientFactory}. If this property
     * wasn't set, will load one of {@link org.apache.iceberg.aws.AwsClientFactory} factory classes to
     * provide backward compatibility.
     */
    public static final String CLIENT_FACTORY = "s3tables.client-factory-impl";

    /**
     * Configure an alternative endpoint of the S3 Tables service to access.
     */
    public static final String S3TABLES_ENDPOINT = "s3tables.endpoint";

    private String s3tablesEndpoint;

    public S3TablesProperties() {
        super();
    }

    public S3TablesProperties(Map<String, String> properties) {
        this.s3tablesEndpoint = properties.get(S3TABLES_ENDPOINT);
    }

    /**
     * Override the endpoint for a s3tables client.
     *
     * <p>Sample usage:
     *
     * <pre>
     *     S3TablesClient.builder().applyMutation(s3TablesProperties::applyS3TableEndpointConfigurations)
     * </pre>
     */
    public <T extends S3TablesClientBuilder> void applyS3TableEndpointConfigurations(T builder) {
        if (s3tablesEndpoint != null) {
            builder.endpointOverride(URI.create(s3tablesEndpoint));
        }
    }
}
