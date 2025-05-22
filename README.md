# Amazon S3 Tables Catalog for Apache Iceberg

<!-- Note for developer: Edit your repository description on GitHub -->

The Amazon S3 Tables Catalog for Apache Iceberg is an open-source library that bridges [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) operations to engines like [Apache Spark](https://spark.apache.org/), when used with the [Apache Iceberg](https://iceberg.apache.org/) Open Table Format. 

This library can: 
* Translate [Apache Iceberg](https://iceberg.apache.org/) operations such as table discovery, metadata reads, and updates
* Add and removes tables in Amazon S3 Tables

<!-- Note for writer: Update the following text after the S3 Tables docs are finalized -->

### What are Amazon S3 Tables and table buckets ?

Amazon S3 Tables are built for storing tabular data, such as daily purchase transactions, streaming sensor data, or ad impressions. Tabular data represents data in columns and rows, like in a database table. Tabular data is most commonly stored in the [Apache Parquet](https://parquet.apache.org/) format.

The tabular data in Amazon S3 Tables is stored in a new S3 bucket type: a **table bucket**, which stores tables as subresources. S3 Tables has built-in support for tables in the [Apache Iceberg](https://iceberg.apache.org/) format. Using standard SQL statements, you can query your tables with query engines that support Apache Iceberg, such as [Amazon Athena](https://aws.amazon.com/athena/), [Amazon Redshift](https://aws.amazon.com/pm/redshift/), and [Apache Spark](https://spark.apache.org/).

## Current Status

Amazon S3 Tables Catalog for Apache Iceberg is generally available. We're always interested in feedback on features, performance, and compatibility. Please send feedback by opening a [GitHub issue](https://github.com/awslabs/s3-tables-catalog/issues/new/).

If you discover a potential security issue in this project we ask that you notify Amazon Web Services (AWS) Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do not create a public GitHub issue.

## Getting Started 

To get started with Amazon S3 Tables, see [Tutorial: Getting started with S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-getting-started.html) in the *Amazon S3 User Guide*. 

### Configuration

- <catalog_name> is your Iceberg Spark session catalog name. Replace it with the name of
your catalog, and remember to change the references throughout all configurations that
are associated with this catalog. In your code, you should then refer to your Iceberg tables
with the fully qualified table name, including the Spark session catalog name, as follows:
<catalog_name>.<database_name>.<table_name>.

- <catalog_name>.warehouse points to the Amazon S3 Tables path
- <catalog_name>.catalog-impl = "software.amazon.s3tables.iceberg.S3TablesCatalog" This key is required to point to an
implementation class for any custom catalog implementation.

### Java Spark app Example

Add the lines below to your pom.xml:
```
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3tables</artifactId>
    <version>2.29.26</version>
</dependency>
<dependency>
    <groupId>software.amazon.s3tables</groupId>
    <artifactId>s3-tables-catalog-for-iceberg</artifactId>
    <version>0.1.6</version>
</dependency>
```
Or if you using a [BOM](https://aws.amazon.com/blogs/developer/managing-dependencies-with-aws-sdk-for-java-bill-of-materials-module-bom/) just add a dependency on the s3 tables sdk:
```
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>bom</artifactId>
            <version>2.29.26</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

Or for Gradle:

```
dependencies {
    implementation 'software.amazon.awssdk:s3tables:2.29.26'
    implementation 'software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.5'
}
```



And finally start a spark session:

```
spark = SparkSession.builder()
            .config("spark.sql.catalog.<catalog_name>", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.<catalog_name>.catalog-impl","software.amazon.s3tables.iceberg.S3TablesCatalog")
            .config("spark.sql.catalog.<catalog_name>.warehouse", <TABLE_BUCKET_ARN>)
            .getOrCreate();
```

## Contributions

We welcome contributions to Amazon S3 Tables Catalog for Apache Iceberg! Please see the [contributing guidelines](CONTRIBUTING.md) for more information on how to report bugs, build from source code, or submit pull requests.

## Security

If you discover a potential security issue in this project we ask that you notify Amazon Web Services (AWS) Security via our [vulnerability reporting](http://aws.amazon.com/security/vulnerability-reporting/) page. Please do not create a public GitHub issue.

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
