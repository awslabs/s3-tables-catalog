package software.amazon.s3tables.iceberg;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class S3TablesLocationProviderTest {

    @Test
    public void testNewDataLocation() {
        S3TablesLocationProvider locationProvider = new S3TablesLocationProvider(
                "s3://dummy-table-bucket/dummy-table", ImmutableMap.of()
        );
        String hash = locationProvider.computeHash("testFile.txt");
        String dataLocation = locationProvider.newDataLocation("testFile.txt");
        assertThat(dataLocation)
                .isEqualTo(String.format("s3://dummy-table-bucket/dummy-table/data/%s-testFile.txt", hash));
    }
}
