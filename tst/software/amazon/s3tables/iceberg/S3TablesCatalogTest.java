package software.amazon.s3tables.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.s3tables.model.ConflictException;
import software.amazon.awssdk.services.s3tables.model.CreateNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.CreateNamespaceResponse;
import software.amazon.awssdk.services.s3tables.model.CreateTableRequest;
import software.amazon.awssdk.services.s3tables.model.CreateTableResponse;
import software.amazon.awssdk.services.s3tables.model.DeleteNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.DeleteNamespaceResponse;
import software.amazon.awssdk.services.s3tables.model.DeleteTableRequest;
import software.amazon.awssdk.services.s3tables.model.DeleteTableResponse;
import software.amazon.awssdk.services.s3tables.model.GetNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.GetNamespaceResponse;
import software.amazon.awssdk.services.s3tables.model.GetTableMetadataLocationRequest;
import software.amazon.awssdk.services.s3tables.model.GetTableMetadataLocationResponse;
import software.amazon.awssdk.services.s3tables.model.ListNamespacesRequest;
import software.amazon.awssdk.services.s3tables.model.ListNamespacesResponse;
import software.amazon.awssdk.services.s3tables.model.ListTablesRequest;
import software.amazon.awssdk.services.s3tables.model.ListTablesResponse;
import software.amazon.awssdk.services.s3tables.model.NamespaceSummary;
import software.amazon.awssdk.services.s3tables.model.NotFoundException;
import software.amazon.awssdk.services.s3tables.model.RenameTableRequest;
import software.amazon.awssdk.services.s3tables.model.RenameTableResponse;
import software.amazon.awssdk.services.s3tables.model.TableSummary;
import software.amazon.awssdk.services.s3tables.model.UpdateTableMetadataLocationRequest;
import software.amazon.awssdk.services.s3tables.model.UpdateTableMetadataLocationResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;

public class S3TablesCatalogTest {
    private static final String DUMMY_WAREHOUSE_PATH = "s3://dummy_warehouse";
    private static final String DUMMY_CATALOG_NAME = "s3tables_catalog";
    private static final String DUMMY_ENDPOINT = "http://localhost:9090";
    private static final TableIdentifier DUMMY_IDENTIFIER = TableIdentifier.of("db", "table");
    private static final String DUMMY_NAMESPACE_NAME = "dummy_namespace";
    private static final String DUMMY_ARN_PATH = "arn:aws:s3tables:us-east-2:012345678901:bucket/example/table/";

    private S3TablesCatalog catalog;
    private S3TablesClient mockClient;
    private S3FileIO mockFileIO;

    @BeforeEach
    public void setup() {
        // To make client construction work without access to IMDS
        System.setProperty("aws.region", "us-east-1");

        mockClient = mock(S3TablesClient.class);
        mockFileIO = mock(S3FileIO.class);
        catalog = new S3TablesCatalog();
        catalog.initialize(DUMMY_CATALOG_NAME,
                ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, DUMMY_WAREHOUSE_PATH),
                mockClient);
    }

    @Test
    public void testThrowsErrorWhenNoWarehouseProvided() {
        try {
            catalog.initialize(DUMMY_CATALOG_NAME, ImmutableMap.of(), mockClient);
        } catch (Exception e) {
            assertThat(e instanceof ValidationException).isTrue();
        }
    }

    @Test
    public void testDefaultWarehouseLocationWhenTableDoesNotExist() {
        CreateTableResponse mockCreateTableResponse = mock(CreateTableResponse.class);
        GetTableMetadataLocationResponse mockGetTableResponse = mock(GetTableMetadataLocationResponse.class);
        TableIdentifier identifier = TableIdentifier.of("dummy-table");
        when(mockClient.createTable(any(CreateTableRequest.class))).thenReturn(mockCreateTableResponse);
        when(mockGetTableResponse.warehouseLocation())
                .thenReturn(String.format("%s/%s", DUMMY_WAREHOUSE_PATH, identifier.name()));
        when(mockClient.getTableMetadataLocation(any(GetTableMetadataLocationRequest.class)))
                .thenThrow(NotFoundException.class)
                .thenReturn(mockGetTableResponse);
        String whLocation = catalog.defaultWarehouseLocation(identifier);
        assertThat(whLocation).isEqualTo(String.format("%s/%s", DUMMY_WAREHOUSE_PATH, identifier.name()));
    }

    @Test
    public void testDefaultWarehouseLocationWhenTableExists() {
        GetTableMetadataLocationResponse mockGetTableResponse = mock(GetTableMetadataLocationResponse.class);
        TableIdentifier identifier = TableIdentifier.of("dummy-table");
        when(mockGetTableResponse.warehouseLocation())
                .thenReturn(String.format("%s/%s", DUMMY_WAREHOUSE_PATH, identifier.name()));
        when(mockClient.getTableMetadataLocation(any(GetTableMetadataLocationRequest.class))).thenReturn(mockGetTableResponse);
        String whLocation = catalog.defaultWarehouseLocation(identifier);
        assertThat(whLocation).isEqualTo(String.format("%s/%s", DUMMY_WAREHOUSE_PATH, identifier.name()));
        verify(mockClient, times(0)).createTable(any(CreateTableRequest.class));
    }

    @Test
    public void testListTables() {
        Namespace namespace = Namespace.of(DUMMY_NAMESPACE_NAME);
        GetNamespaceResponse mockNamespaceresponse = mock(GetNamespaceResponse.class);
        when(mockNamespaceresponse.toString()).thenReturn(namespace.toString());
        when(mockClient.getNamespace(any(GetNamespaceRequest.class)))
                .thenReturn(mockNamespaceresponse);

        TableSummary[] tableSummaryResponses = new TableSummary[]{
          TableSummary.builder().tableARN(DUMMY_ARN_PATH+"table1").name("table1").build(),
          TableSummary.builder().tableARN(DUMMY_ARN_PATH+"table2").name("table2").build(),
          TableSummary.builder().tableARN(DUMMY_ARN_PATH+"table3").name("table3").build(),
          TableSummary.builder().tableARN(DUMMY_ARN_PATH+"table4").name("table4").build(),
          TableSummary.builder().tableARN(DUMMY_ARN_PATH+"table5").name("table5").build()};

        ListTablesResponse response1 = ListTablesResponse.builder()
                .tables(Arrays.asList(tableSummaryResponses[0], tableSummaryResponses[1], tableSummaryResponses[2]))
            .continuationToken("token")
                .build();

        ListTablesResponse response2 = ListTablesResponse.builder()
            .tables(Arrays.asList(tableSummaryResponses[3], tableSummaryResponses[4]))
            .build();

        when(mockClient.listTables(any(ListTablesRequest.class))).thenReturn(response1).thenReturn(response2);

        List<TableIdentifier> identifierList = catalog.listTables(namespace);
        verify(mockClient, times(2)).listTables(any(ListTablesRequest.class));
        assertThat(identifierList.size()).isEqualTo(5);

        for(int i =0; i < identifierList.size(); i++) {
          assertThat(identifierList.get(i).name()).isEqualTo(tableSummaryResponses[i].name());
        }
    }

    @Test
    public void testCreateNamespace() {
        when(mockClient.createNamespace(any(CreateNamespaceRequest.class)))
                .thenReturn(CreateNamespaceResponse.builder()
                        .namespace(DUMMY_NAMESPACE_NAME)
                        .tableBucketARN(DUMMY_ARN_PATH+"DUMMY_NAME")
                        .build());
        catalog.createNamespace(Namespace.of(DUMMY_NAMESPACE_NAME), ImmutableMap.of());
        verify(mockClient, times(1)).createNamespace(any(CreateNamespaceRequest.class));
    }

  @Test
  public void testListNamespaces() {
    NamespaceSummary[] namespaceSummaryResponses = new NamespaceSummary[] {
          NamespaceSummary.builder().namespace("ns1").build(),
          NamespaceSummary.builder().namespace("ns3").build(),
          NamespaceSummary.builder().namespace("ns5").build(),
          NamespaceSummary.builder().namespace("ns6").build(),
          NamespaceSummary.builder().namespace("ns7").build(),
          NamespaceSummary.builder().namespace("ns8").build()
    };

    when(mockClient.listNamespaces(any(ListNamespacesRequest.class)))
        .thenReturn(ListNamespacesResponse.builder()
            .namespaces(Arrays.asList(namespaceSummaryResponses[0], namespaceSummaryResponses[1], namespaceSummaryResponses[2]))
            .continuationToken("token")
            .build())
        .thenReturn(ListNamespacesResponse.builder()
            .namespaces(Arrays.asList(namespaceSummaryResponses[3], namespaceSummaryResponses[4], namespaceSummaryResponses[5]
            )).build());

    List<Namespace> namespaceList = catalog.listNamespaces(Namespace.empty());
    verify(mockClient, times(2)).listNamespaces(any(ListNamespacesRequest.class));
    assertThat(namespaceList.size()).isEqualTo(6);

    for(int i =0; i < namespaceList.size(); i++) {
      assertThat(namespaceList.get(i).levels()[0].equals(namespaceSummaryResponses[i].namespace().get(0)));
    }
  }

    @Test
    public void testDropTable() {
        when(mockClient.getTableMetadataLocation(any(GetTableMetadataLocationRequest.class))).thenReturn(mock(GetTableMetadataLocationResponse.class));
        when(mockClient.deleteTable(any(DeleteTableRequest.class)))
                .thenReturn(DeleteTableResponse.builder().build());
        assertThat(catalog.dropTable(TableIdentifier.of("dummy-table"), true)).isTrue();
    }

  @Test
  public void testDropTableWithoutPurgeThrowsException() {
    when(mockClient.getTableMetadataLocation(any(GetTableMetadataLocationRequest.class))).thenReturn(mock(GetTableMetadataLocationResponse.class));
    when(mockClient.deleteTable(any(DeleteTableRequest.class)))
        .thenReturn(DeleteTableResponse.builder().build());
    assertThrows(UnsupportedOperationException.class, () -> catalog.dropTable(TableIdentifier.of("dummy-table"), false));
  }

    @Test
    public void testRenameTable() {
        ArgumentCaptor<RenameTableRequest> requestCaptor = ArgumentCaptor.forClass(RenameTableRequest.class);

        when(mockClient.getTableMetadataLocation(any(GetTableMetadataLocationRequest.class))).thenReturn(mock(GetTableMetadataLocationResponse.class));
        when(mockClient.updateTableMetadataLocation(any(UpdateTableMetadataLocationRequest.class))).thenReturn(mock(UpdateTableMetadataLocationResponse.class));

        when(mockClient.renameTable(any(RenameTableRequest.class))).thenReturn(mock(RenameTableResponse.class));
        catalog.renameTable(TableIdentifier.of("sourcenamespace","sourcetable"),
                TableIdentifier.of("targetnamespace", "targettable"));
        verify(mockClient, times(1)).renameTable(requestCaptor.capture());
        RenameTableRequest capturedRequest = requestCaptor.getValue();
        assertThat(capturedRequest.newNamespaceName()).isEqualTo("targetnamespace");
        assertThat(capturedRequest.newName()).isEqualTo("targettable");
    }

    @Test
    public void testSetProperties() {
        when(mockClient.getNamespace(any(GetNamespaceRequest.class)))
                .thenReturn(GetNamespaceResponse.builder()
                        .namespace(DUMMY_NAMESPACE_NAME).build());
        assertThat(catalog.setProperties(Namespace.of(DUMMY_NAMESPACE_NAME), ImmutableMap.of())).isTrue();
    }

    @Test
    public void testRemoveProperties() {
        when(mockClient.getNamespace(any(GetNamespaceRequest.class)))
                .thenReturn(GetNamespaceResponse.builder()
                        .namespace(DUMMY_NAMESPACE_NAME).build());
        assertThat(catalog.removeProperties(Namespace.of(DUMMY_NAMESPACE_NAME), ImmutableSet.of())).isTrue();
    }

    @Test
    public void testTableLevelS3Properties() {
        Map<String, String> properties =
            ImmutableMap.of(
                S3TablesProperties.S3TABLES_ENDPOINT,
                DUMMY_ENDPOINT,
                AwsClientProperties.CLIENT_REGION,
                "us-east-1",
                AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                DUMMY_ENDPOINT + "authorizer",
                CatalogProperties.WAREHOUSE_LOCATION, DUMMY_WAREHOUSE_PATH);

        catalog.initialize(DUMMY_CATALOG_NAME, properties, mockClient);
        S3TablesCatalogOperations ops = (S3TablesCatalogOperations) catalog.newTableOps(DUMMY_IDENTIFIER);

        Map<String, String> tableCatalogProperties = ops.tableCatalogProperties();

        assertThat(tableCatalogProperties)
            .containsEntry(
                S3TablesProperties.S3TABLES_ENDPOINT,
                DUMMY_ENDPOINT)
            .containsEntry(
                AwsClientProperties.CLIENT_REGION,
                "us-east-1").containsEntry(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                DUMMY_ENDPOINT + "authorizer");
    }

    @Test
    public void testDefaultLocationProvider() {
        TableOperations mockTableOperations = mock(S3TablesCatalogOperations.class);
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        when(mockTableMetadata.location()).thenReturn("s3://amzn-s3-demo-dummybucket/");
        when(mockTableMetadata.properties()).thenReturn(ImmutableMap.of());
        when(mockTableOperations.current()).thenReturn(mockTableMetadata);
        when(mockTableOperations.locationProvider()).thenCallRealMethod();
        LocationProvider locationProvider = mockTableOperations.locationProvider();
        assertThat(locationProvider instanceof S3TablesLocationProvider).isTrue();
    }

    public static class TestLocationProvider implements LocationProvider {
        public TestLocationProvider() {}

        @Override
        public String newDataLocation(String s) {
            return "dummylocation";
        }

        @Override
        public String newDataLocation(PartitionSpec partitionSpec, StructLike structLike, String s) {
            return "dummylocation";
        }
    }

    @Test
    public void testOverriddenLocationProvider() {
        TableOperations mockTableOperations = mock(S3TablesCatalogOperations.class);
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        when(mockTableMetadata.location()).thenReturn("s3://amzn-s3-demo-dummybucket/");
        when(mockTableMetadata.properties()).thenReturn(ImmutableMap.of(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, "software.amazon.s3tables.iceberg.S3TablesCatalogTest$TestLocationProvider"));
        when(mockTableOperations.current()).thenReturn(mockTableMetadata);
        when(mockTableOperations.locationProvider()).thenCallRealMethod();
        LocationProvider locationProvider = mockTableOperations.locationProvider();
        assertThat(locationProvider instanceof TestLocationProvider).isTrue();
    }

    @Test
    public void testObjectStoreEnabledLocationProvider() {
        TableOperations mockTableOperations = mock(S3TablesCatalogOperations.class);
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        when(mockTableMetadata.location()).thenReturn("s3://amzn-s3-demo-dummybucket/");
        when(mockTableMetadata.properties()).thenReturn(ImmutableMap.of(TableProperties.OBJECT_STORE_ENABLED, "true"));
        when(mockTableOperations.current()).thenReturn(mockTableMetadata);
        when(mockTableOperations.locationProvider()).thenCallRealMethod();
        LocationProvider locationProvider = mockTableOperations.locationProvider();
        assertThat(locationProvider instanceof S3TablesLocationProvider).isTrue();
    }

    @Test
    public void testObjectStoreDisabledLocationProvider() {
        TableOperations mockTableOperations = mock(S3TablesCatalogOperations.class);
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        when(mockTableMetadata.location()).thenReturn("s3://amzn-s3-demo-dummybucket/");
        when(mockTableMetadata.properties()).thenReturn(ImmutableMap.of(TableProperties.OBJECT_STORE_ENABLED, "false"));
        when(mockTableOperations.current()).thenReturn(mockTableMetadata);
        when(mockTableOperations.locationProvider()).thenCallRealMethod();
        LocationProvider locationProvider = mockTableOperations.locationProvider();
        // Iceberg's default location providers aren't public
        assertThat(locationProvider.getClass().getName()).isEqualTo("org.apache.iceberg.LocationProviders$DefaultLocationProvider");
    }

    @Test
    public void testDeleteNamespace() {
        Namespace namespace = Namespace.of(DUMMY_NAMESPACE_NAME);
        GetNamespaceResponse mockNamespaceresponse = mock(GetNamespaceResponse.class);
        when(mockNamespaceresponse.toString()).thenReturn(namespace.toString());
        when(mockNamespaceresponse.namespace()).thenReturn(Arrays.asList(DUMMY_NAMESPACE_NAME));
        when(mockClient.getNamespace(any(GetNamespaceRequest.class)))
            .thenReturn(mockNamespaceresponse);
        when(mockClient.deleteNamespace(any(DeleteNamespaceRequest.class)))
            .thenReturn(DeleteNamespaceResponse.builder().build());

        assertThat(catalog.dropNamespace(Namespace.of(DUMMY_NAMESPACE_NAME))).isTrue();
        verify(mockClient, times(1)).deleteNamespace(any(DeleteNamespaceRequest.class));
    }

  @Test
  public void testDeleteNamespaceThrowsExpectedErrors() {
    Namespace namespace = Namespace.of(DUMMY_NAMESPACE_NAME);
    GetNamespaceResponse mockNamespaceresponse = mock(GetNamespaceResponse.class);
    when(mockNamespaceresponse.toString()).thenReturn(namespace.toString());
    when(mockNamespaceresponse.namespace()).thenReturn(Arrays.asList(DUMMY_NAMESPACE_NAME));
    when(mockClient.getNamespace(any(GetNamespaceRequest.class)))
        .thenReturn(mockNamespaceresponse);
    when(mockClient.deleteNamespace(any(DeleteNamespaceRequest.class)))
        .thenThrow(ConflictException.builder().build());

    assertThrows(ConflictException.class, () -> catalog.dropNamespace(Namespace.of(DUMMY_NAMESPACE_NAME)));
    verify(mockClient, times(1)).deleteNamespace(any(DeleteNamespaceRequest.class));

    when(mockClient.deleteNamespace(any(DeleteNamespaceRequest.class)))
        .thenThrow(new IllegalArgumentException("blah"));

    assertThrows(IllegalArgumentException.class, () -> catalog.dropNamespace(Namespace.of(DUMMY_NAMESPACE_NAME)));
  }

    @Test
    public void testValidateSingleLevelNamespace() {
      assertDoesNotThrow(() -> S3TablesCatalog.validateSingleLevelNamespace(Namespace.of("level1", "level2"), 2));
      assertThrows(ValidationException.class,() -> S3TablesCatalog.validateSingleLevelNamespace(Namespace.of("level1", "level2")));
    }

    @Test
    public void testUpdateMetadataExceptionHandling() {
        // simulate an empty table to avoid TableMetadata reading
        GetTableMetadataLocationResponse getTableResponse = mock(GetTableMetadataLocationResponse.class);
        when(getTableResponse.metadataLocation()).thenReturn(null);
        when(mockClient.getTableMetadataLocation(any(GetTableMetadataLocationRequest.class)))
                .thenThrow(NotFoundException.class)
                .thenReturn(getTableResponse);

        when(mockClient.updateTableMetadataLocation(any(UpdateTableMetadataLocationRequest.class)))
                .thenThrow(ConflictException.class);
        TableMetadata newTableMetadata = mock(TableMetadata.class);
        when(newTableMetadata.metadataFileLocation()).thenReturn("x");

        assertThrows(
                CommitFailedException.class, 
                () -> catalog.newTableOps(TableIdentifier.of("a", "b"))
                        .commit(null, newTableMetadata)
        );
    }

    @Test
    public void testInitializeClientFactory() {
        S3TablesCatalog catalog = new S3TablesCatalog();
        catalog.initialize("testcatalog", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, DUMMY_WAREHOUSE_PATH));
    }

    static class TestS3TablesClient implements S3TablesClient {
        @Override
        public String serviceName() {
            return "s3tables";
        }

        @Override
        public void close() {}
    }

    static class TestS3TablesAwsClientFactory implements S3TablesAwsClientFactory {
        @Override
        public S3TablesClient s3tables() {
            return new TestS3TablesClient();
        }

        @Override
        public void initialize(Map<String, String> properties) {}
    }

    @Test
    public void testInitializeClientFactoryCustom() {
        S3TablesCatalog catalog = new S3TablesCatalog();
        catalog.initialize("testcatalog", ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION, DUMMY_WAREHOUSE_PATH,
                S3TablesProperties.CLIENT_FACTORY, TestS3TablesAwsClientFactory.class.getName()));

        S3TablesClient client = catalog.getS3TablesClient();
        assertThat(client).isInstanceOf(TestS3TablesClient.class);
    }
}
