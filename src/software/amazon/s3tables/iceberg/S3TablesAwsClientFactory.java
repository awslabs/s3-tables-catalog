package software.amazon.s3tables.iceberg;

import software.amazon.awssdk.services.s3tables.S3TablesClient;

import java.io.Serializable;
import java.util.Map;

public interface S3TablesAwsClientFactory extends Serializable {
    /**
     * create a Amazon S3 Tables client
     *
     * @return s3tables client
     */
    S3TablesClient s3tables();

    /**
     * Initialize AWS client factory from catalog properties.
     *
     * @param properties catalog properties
     */
    void initialize(Map<String, String> properties);
}
