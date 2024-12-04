package software.amazon.s3tables.iceberg;

import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.s3tables.iceberg.imports.AwsClientProperties;
import software.amazon.s3tables.iceberg.imports.HttpClientProperties;

import java.util.Map;

public class S3TablesAwsClientFactories {

    private S3TablesAwsClientFactories() {
    }

    public static S3TablesAwsClientFactory from(Map<String, String> properties) {
        String factoryImpl = PropertyUtil.propertyAsString(properties, S3TablesProperties.CLIENT_FACTORY, DefaultS3TablesAwsClientFactory.class.getName());
        return loadClientFactory(factoryImpl, properties);
    }

    private static S3TablesAwsClientFactory loadClientFactory(String impl, Map<String, String> properties) {
        DynConstructors.Ctor<S3TablesAwsClientFactory> ctor;
        try {
            ctor = DynConstructors.builder(S3TablesAwsClientFactory.class).loader(S3TablesAwsClientFactories.class.getClassLoader()).hiddenImpl(impl).buildChecked();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(String.format("Cannot initialize S3TablesAwsClientFactory, missing no-arg constructor: %s", impl), e);
        }

        S3TablesAwsClientFactory factory;
        try {
            factory = ctor.newInstance();
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format("Cannot initialize S3TablesAwsClientFactory, %s does not implement S3TablesAwsClientFactory.", impl), e);
        }

        factory.initialize(properties);
        return factory;
    }

    public static class DefaultS3TablesAwsClientFactory implements S3TablesAwsClientFactory {
        protected S3TablesProperties s3TablesProperties;
        protected AwsClientProperties awsClientProperties;
        protected HttpClientProperties httpClientProperties;

        public DefaultS3TablesAwsClientFactory() {
            s3TablesProperties = new S3TablesProperties();
            awsClientProperties = new AwsClientProperties();
            httpClientProperties = new HttpClientProperties();
        }

        @Override
        public void initialize(Map<String, String> properties) {
            this.s3TablesProperties = new S3TablesProperties(properties);
            this.awsClientProperties = new AwsClientProperties(properties);
            this.httpClientProperties = new HttpClientProperties(properties);
        }

        @Override
        public S3TablesClient s3tables() {
            return S3TablesClient.builder()
                    .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                    .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                    .applyMutation(s3TablesProperties::applyUserAgentConfigurations)
                    .applyMutation(s3TablesProperties::applyS3TableEndpointConfigurations)
                    .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                    .build();
        }
    }
}
