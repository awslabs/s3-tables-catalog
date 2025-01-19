package software.amazon.s3tables.iceberg.rest;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

@Configuration
public class S3TablesRestCatalogConfig {
    @Bean
    public S3TablesCatalog s3TablesCatalog() {
        return new S3TablesCatalog();
    }
}
