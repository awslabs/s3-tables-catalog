package software.amazon.s3tables.iceberg.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class S3TablesRestCatalogServer {
    public static void main(String[] args) {
        SpringApplication.run(S3TablesRestCatalogServer.class, args);
    }
}
