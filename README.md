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

To get started with Amazon S3 Tables Catalog for Apache Iceberg, ...

Import

```
<replace with code>
```

Other getting started step ...

```
<replace with other code>
```

## Contributions

We welcome contributions to Amazon S3 Tables Catalog for Apache Iceberg! Please see the [contributing guidelines](CONTRIBUTING.md) for more information on how to report bugs, build from source code, or submit pull requests.

## Security

If you discover a potential security issue in this project we ask that you notify Amazon Web Services (AWS) Security via our [vulnerability reporting](http://aws.amazon.com/security/vulnerability-reporting/) page. Please do not create a public GitHub issue.

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
