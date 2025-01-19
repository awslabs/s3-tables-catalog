package software.amazon.s3tables.iceberg.rest;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/v1")
public class S3TablesRestCatalogController {
    @Autowired
    private S3TablesCatalog catalog;

    @GetMapping("/config")
    public ResponseEntity<Map<String, String>> config() {
        Map<String, String> config = new HashMap<>();
        config.put("catalog-version", "1.5.0");
        config.put("catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog");
        config.put("warehouse", catalog.name());
        return ResponseEntity.ok(config);
    }

    @PostMapping("/namespaces")
    public ResponseEntity<Map<String, Object>> createNamespace(@RequestBody Map<String, Object> request) {
        String[] namespace = ((String) request.get("namespace")).split("\\.");
        @SuppressWarnings("unchecked")
        Map<String, String> properties = (Map<String, String>) request.get("properties");
        
        catalog.createNamespace(Namespace.of(namespace), properties);
        
        Map<String, Object> response = new HashMap<>();
        response.put("namespace", String.join(".", namespace));
        response.put("properties", properties);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/namespaces")
    public ResponseEntity<List<Map<String, Object>>> listNamespaces(@RequestParam(required = false) String parent) {
        List<Namespace> namespaces = catalog.listNamespaces(
            parent != null ? Namespace.of(parent.split("\\.")) : null
        );
        
        List<Map<String, Object>> response = namespaces.stream()
            .map(ns -> {
                Map<String, Object> item = new HashMap<>();
                item.put("namespace", String.join(".", ns.levels()));
                item.put("metadata", catalog.loadNamespaceMetadata(ns));
                return item;
            })
            .collect(Collectors.toList());
            
        return ResponseEntity.ok(response);
    }

    @GetMapping("/namespaces/{namespace}")
    public ResponseEntity<Map<String, Object>> loadNamespace(@PathVariable String namespace) {
        Namespace ns = Namespace.of(namespace.split("\\."));
        Map<String, String> metadata = catalog.loadNamespaceMetadata(ns);
        
        Map<String, Object> response = new HashMap<>();
        response.put("namespace", namespace);
        response.put("metadata", metadata);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/tables")
    public ResponseEntity<Map<String, Object>> createTable(@RequestBody Map<String, Object> request) {
        String[] namespace = ((String) request.get("namespace")).split("\\.");
        String name = (String) request.get("name");
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace), name);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> schema = (Map<String, Object>) request.get("schema");
        @SuppressWarnings("unchecked")
        Map<String, Object> spec = (Map<String, Object>) request.getOrDefault("spec", null);
        @SuppressWarnings("unchecked")
        Map<String, String> properties = (Map<String, String>) request.getOrDefault("properties", new HashMap<>());
        
        org.apache.iceberg.Schema icebergSchema = org.apache.iceberg.SchemaParser.fromJson(schema.toString());
        org.apache.iceberg.PartitionSpec partitionSpec = spec != null ? 
            org.apache.iceberg.PartitionSpecParser.fromJson(icebergSchema, spec.toString()) :
            org.apache.iceberg.PartitionSpec.unpartitioned();
        
        org.apache.iceberg.Table table = catalog.createTable(
            identifier,
            icebergSchema,
            partitionSpec,
            properties
        );
        
        Map<String, Object> response = new HashMap<>();
        response.put("namespace", String.join(".", namespace));
        response.put("name", name);
        response.put("schema", org.apache.iceberg.SchemaParser.toJson(table.schema()));
        response.put("spec", org.apache.iceberg.PartitionSpecParser.toJson(table.spec()));
        response.put("properties", table.properties());
        response.put("location", table.location());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/tables")
    public ResponseEntity<List<Map<String, Object>>> listTables(@RequestParam(required = false) String namespace) {
        List<TableIdentifier> tables = catalog.listTables(
            namespace != null ? Namespace.of(namespace.split("\\.")) : null
        );
        
        List<Map<String, Object>> response = tables.stream()
            .map(table -> {
                Map<String, Object> item = new HashMap<>();
                item.put("namespace", String.join(".", table.namespace().levels()));
                item.put("name", table.name());
                return item;
            })
            .collect(Collectors.toList());
            
        return ResponseEntity.ok(response);
    }

    @GetMapping("/tables/{namespace}/{table}")
    public ResponseEntity<Map<String, Object>> loadTable(
            @PathVariable String namespace,
            @PathVariable String table) {
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
        org.apache.iceberg.Table icebergTable = catalog.loadTable(identifier);
        
        Map<String, Object> response = new HashMap<>();
        response.put("namespace", namespace);
        response.put("name", table);
        response.put("format-version", icebergTable.properties().getOrDefault("format-version", "1"));
        response.put("location", icebergTable.location());
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/tables/{namespace}/{table}")
    public ResponseEntity<Void> dropTable(
            @PathVariable String namespace,
            @PathVariable String table,
            @RequestParam(required = false, defaultValue = "false") boolean purge) {
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
        catalog.dropTable(identifier, purge);
        return ResponseEntity.ok().build();
    }
}
