package software.amazon.s3tables.iceberg;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.s3tables.iceberg.imports.AwsClientProperties;
import software.amazon.s3tables.iceberg.imports.HttpClientProperties;

import java.util.Map;
import java.util.UUID;

public class S3TablesAssumeRoleAwsClientFactory implements S3TablesAwsClientFactory {
    protected S3TablesProperties s3TablesProperties;
    protected AwsClientProperties awsClientProperties;
    protected HttpClientProperties httpClientProperties;
    private String roleSessionName;

    public S3TablesAssumeRoleAwsClientFactory() {
        s3TablesProperties = new S3TablesProperties();
        awsClientProperties = new AwsClientProperties();
        httpClientProperties = new HttpClientProperties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        s3TablesProperties = new S3TablesProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
        awsClientProperties = new AwsClientProperties(properties);
        this.roleSessionName = genSessionName();
        Preconditions.checkNotNull(
                awsClientProperties.clientAssumeRoleArn(),
                "Cannot initialize AssumeRoleClientConfigFactory with null role ARN");
        Preconditions.checkNotNull(
                awsClientProperties.clientAssumeRoleRegion(),
                "Cannot initialize AssumeRoleClientConfigFactory with null region");
    }

    @Override
    public S3TablesClient s3tables() {
        return S3TablesClient.builder()
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(s3TablesProperties::applyS3TableEndpointConfigurations)
                .applyMutation(this::applyAssumeRoleConfigurations)
                .build();
    }

    private String genSessionName() {
        return String.format("s3tables-aws-%s", UUID.randomUUID());
    }

    protected <T extends AwsClientBuilder & AwsSyncClientBuilder> T applyAssumeRoleConfigurations(
            T clientBuilder) {
        AssumeRoleRequest assumeRoleRequest =
                AssumeRoleRequest.builder()
                        .roleArn(awsClientProperties.clientAssumeRoleArn())
                        .roleSessionName(roleSessionName)
                        .durationSeconds(awsClientProperties.clientAssumeRoleTimeoutSec())
                        .externalId(awsClientProperties.clientAssumeRoleExternalId())
                        .tags(awsClientProperties.stsClientAssumeRoleTags())
                        .build();
        clientBuilder
                .credentialsProvider(
                        StsAssumeRoleCredentialsProvider.builder()
                                .stsClient(sts())
                                .refreshRequest(assumeRoleRequest)
                                .build())
                .region(Region.of(awsClientProperties.clientAssumeRoleRegion()));
        return clientBuilder;
    }

    private StsClient sts() {
        return StsClient.builder()
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .build();
    }
}
