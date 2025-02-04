/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package software.amazon.s3tables.iceberg.imports;

import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.model.Tag;
import software.amazon.s3tables.iceberg.S3TablesAssumeRoleAwsClientFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AwsClientProperties implements Serializable {
  /**
   * Configure the AWS credentials provider used to create AWS clients. A fully qualified concrete
   * class with package that implements the {@link AwsCredentialsProvider} interface is required.
   *
   * <p>Additionally, the implementation class must also have a create() or create(Map) method
   * implemented, which returns an instance of the class that provides aws credentials provider.
   *
   * <p>Example:
   * client.credentials-provider=software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
   *
   * <p>When set, the default client factory {@link
   * software.amazon.s3tables.iceberg.S3TablesAwsClientFactory} will use this provider to get AWS credentials
   * provided instead of reading the default credential chain to get AWS access credentials.
   */
  public static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";

  /**
   * Used by the client.credentials-provider configured value that will be used by {@link
   * software.amazon.s3tables.iceberg.S3TablesAwsClientFactory}
   * to pass provider-specific properties. Each property consists of a key name and an
   * associated value.
   */
  protected static final String CLIENT_CREDENTIAL_PROVIDER_PREFIX = "client.credentials-provider.";

  /**
   * Used by {@link software.amazon.s3tables.iceberg.S3TablesAwsClientFactory}.
   * If set, all AWS clients except STS client will use the given
   * region instead of the default region chain.
   */
  public static final String CLIENT_REGION = "client.region";

  /**
   * Used by {@link S3TablesAssumeRoleAwsClientFactory}. If set, all AWS clients will assume a role of the
   * given ARN, instead of using the default credential chain.
   */
  public static final String CLIENT_ASSUME_ROLE_ARN = "client.assume-role.arn";


  /**
   * Used by {@link S3TablesAssumeRoleAwsClientFactory}. Optional external ID used to assume an IAM role.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
   */
  public static final String CLIENT_ASSUME_ROLE_EXTERNAL_ID = "client.assume-role.external-id";

  /**
   * Used by {@link S3TablesAssumeRoleAwsClientFactory}. If set, all AWS clients except STS client will use
   * the given region instead of the default region chain.
   *
   * <p>The value must be one of {@link software.amazon.awssdk.regions.Region}, such as 'us-east-1'.
   * For more details, see https://docs.aws.amazon.com/general/latest/gr/rande.html
   */
  public static final String CLIENT_ASSUME_ROLE_REGION = "client.assume-role.region";

  /**
   * Used by {@link S3TablesAssumeRoleAwsClientFactory}. The timeout of the assume role session in seconds,
   * default to 1 hour. At the end of the timeout, a new set of role session credentials will be
   * fetched through a STS client.
   */
  public static final String CLIENT_ASSUME_ROLE_TIMEOUT_SEC = "client.assume-role.timeout-sec";

  public static final int CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT = 3600;

  /**
   * Used by {@link S3TablesAssumeRoleAwsClientFactory}. Optional session name used to assume an IAM role.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html#ck_rolesessionname
   */
  public static final String CLIENT_ASSUME_ROLE_SESSION_NAME = "client.assume-role.session-name";

  /**
   * Used by {@link S3TablesAssumeRoleAwsClientFactory} to pass a list of sessions. Each session tag
   * consists of a key name and an associated value.
   */
  public static final String CLIENT_ASSUME_ROLE_TAGS_PREFIX = "client.assume-role.tags.";

  private final Set<Tag> stsClientAssumeRoleTags;

  private String clientRegion;
  private final String clientCredentialsProvider;
  private final Map<String, String> clientCredentialsProviderProperties;

  private final String clientAssumeRoleArn;
  private final String clientAssumeRoleExternalId;
  private final String clientAssumeRoleRegion;
  private final String clientAssumeRoleSessionName;
  private final int clientAssumeRoleTimeoutSec;

  public AwsClientProperties() {
    this.stsClientAssumeRoleTags = Sets.newHashSet();
    this.clientRegion = null;
    this.clientCredentialsProvider = null;
    this.clientCredentialsProviderProperties = null;

    this.clientAssumeRoleArn = null;
    this.clientAssumeRoleExternalId = null;
    this.clientAssumeRoleRegion = null;
    this.clientAssumeRoleSessionName = null;
    this.clientAssumeRoleTimeoutSec = CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT;
  }

  public String clientAssumeRoleArn() {
    return clientAssumeRoleArn;
  }

  public String clientAssumeRoleRegion() {
    return clientAssumeRoleRegion;
  }

  public int clientAssumeRoleTimeoutSec() {
    return clientAssumeRoleTimeoutSec;
  }

  public String clientAssumeRoleExternalId() {
    return clientAssumeRoleExternalId;
  }

  public AwsClientProperties(Map<String, String> properties) {
    this.clientRegion = properties.get(CLIENT_REGION);
    this.clientCredentialsProvider = properties.get(CLIENT_CREDENTIALS_PROVIDER);
    this.clientCredentialsProviderProperties =
        PropertyUtil.propertiesWithPrefix(properties, CLIENT_CREDENTIAL_PROVIDER_PREFIX);
    this.clientAssumeRoleArn = properties.get(CLIENT_ASSUME_ROLE_ARN);
    this.clientAssumeRoleExternalId = properties.get(CLIENT_ASSUME_ROLE_EXTERNAL_ID);
    this.clientAssumeRoleTimeoutSec =
            PropertyUtil.propertyAsInt(
                    properties, CLIENT_ASSUME_ROLE_TIMEOUT_SEC, CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT);
    this.clientAssumeRoleRegion = properties.get(CLIENT_ASSUME_ROLE_REGION);
    this.clientAssumeRoleSessionName = properties.get(CLIENT_ASSUME_ROLE_SESSION_NAME);
    this.stsClientAssumeRoleTags = toStsTags(properties, CLIENT_ASSUME_ROLE_TAGS_PREFIX);
  }

  public String clientRegion() {
    return clientRegion;
  }

  public void setClientRegion(String clientRegion) {
    this.clientRegion = clientRegion;
  }

  public Set<software.amazon.awssdk.services.sts.model.Tag> stsClientAssumeRoleTags() {
    return stsClientAssumeRoleTags;
  }

  private Set<software.amazon.awssdk.services.sts.model.Tag> toStsTags(
          Map<String, String> properties, String prefix) {
    return PropertyUtil.propertiesWithPrefix(properties, prefix).entrySet().stream()
            .map(
                    e ->
                            software.amazon.awssdk.services.sts.model.Tag.builder()
                                    .key(e.getKey())
                                    .value(e.getValue())
                                    .build())
            .collect(Collectors.toSet());
  }
  /**
   * Configure a client AWS region.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsClientProperties::applyClientRegionConfiguration)
   * </pre>
   */
  public <T extends AwsClientBuilder> void applyClientRegionConfiguration(T builder) {
    if (clientRegion != null) {
      builder.region(Region.of(clientRegion));
    }
  }

  /**
   * Configure the credential provider for AWS clients.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     DynamoDbClient.builder().applyMutation(awsClientProperties::applyClientCredentialConfigurations)
   * </pre>
   */
  public <T extends AwsClientBuilder> void applyClientCredentialConfigurations(T builder) {
    if (!Strings.isNullOrEmpty(this.clientCredentialsProvider)) {
      builder.credentialsProvider(credentialsProvider(this.clientCredentialsProvider));
    }
  }

  /**
   * Returns a credentials provider instance. If params were set, we return a new
   * credentials instance. If none of the params are set, we try to dynamically load the provided
   * credentials provider class. Upon loading the class, we try to invoke {@code create(Map<String,
   * String>)} static method. If that fails, we fall back to {@code create()}. If credential
   * provider class wasn't set, we fall back to default credentials provider.
   *
   * @param accessKeyId the AWS access key ID
   * @param secretAccessKey the AWS secret access key
   * @param sessionToken the AWS session token
   * @return a credentials provider instance
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public AwsCredentialsProvider credentialsProvider(
      String accessKeyId, String secretAccessKey, String sessionToken) {

    if (!Strings.isNullOrEmpty(accessKeyId) && !Strings.isNullOrEmpty(secretAccessKey)) {
      if (Strings.isNullOrEmpty(sessionToken)) {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, secretAccessKey));
      } else {
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
      }
    }

    if (!Strings.isNullOrEmpty(this.clientCredentialsProvider)) {
      return credentialsProvider(this.clientCredentialsProvider);
    }

    // Create a new credential provider for each client
    return DefaultCredentialsProvider.builder().build();
  }

  private AwsCredentialsProvider credentialsProvider(String credentialsProviderClass) {
    Class<?> providerClass;
    try {
      providerClass = DynClasses.builder().impl(credentialsProviderClass).buildChecked();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot load class %s, it does not exist in the classpath", credentialsProviderClass),
          e);
    }

    Preconditions.checkArgument(
        AwsCredentialsProvider.class.isAssignableFrom(providerClass),
        String.format(
            "Cannot initialize %s, it does not implement %s.",
            credentialsProviderClass, AwsCredentialsProvider.class.getName()));

    try {
      return createCredentialsProvider(providerClass);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create an instance of %s, it does not contain a static 'create' or 'create(Map<String, String>)' method",
              credentialsProviderClass),
          e);
    }
  }

  private AwsCredentialsProvider createCredentialsProvider(Class<?> providerClass)
      throws NoSuchMethodException {
    AwsCredentialsProvider provider;
    try {
      provider =
          DynMethods.builder("create")
              .hiddenImpl(providerClass, Map.class)
              .buildStaticChecked()
              .invoke(clientCredentialsProviderProperties);
    } catch (NoSuchMethodException e) {
      provider =
          DynMethods.builder("create").hiddenImpl(providerClass).buildStaticChecked().invoke();
    }
    return provider;
  }
}
