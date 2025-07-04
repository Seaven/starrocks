// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.connector.share.iceberg.IcebergAwsClientFactory;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class IcebergAwsClientFactoryTest {
    @Test
    public void testAKSK() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "ak");
        properties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, "sk");
        properties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, "endpoint");
        properties.put(CloudConfigurationConstants.AWS_S3_REGION, "xxx");

        properties.put(CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY, "ak");
        properties.put(CloudConfigurationConstants.AWS_GLUE_SECRET_KEY, "sk");
        properties.put(CloudConfigurationConstants.AWS_GLUE_ENDPOINT, "endpoint");
        properties.put(CloudConfigurationConstants.AWS_GLUE_REGION, "region");
        IcebergAwsClientFactory factory = new IcebergAwsClientFactory();
        factory.initialize(properties);
        Assertions.assertNotNull(factory.s3());
        Assertions.assertNotNull(factory.s3Async());
        Assertions.assertNotNull(factory.glue());

        Assertions.assertNull(factory.dynamo());
        Assertions.assertNull(factory.kms());
    }

    @Test
    public void testVendedCredentials() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "ak");
        properties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, "sk");
        properties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, "endpoint");
        properties.put(CloudConfigurationConstants.AWS_S3_REGION, "xxx");
        IcebergAwsClientFactory factory = new IcebergAwsClientFactory();
        factory.initialize(properties);
        Assertions.assertNotNull(factory.s3());
        // test vended credentials
        properties = new HashMap<>();
        properties.put(S3FileIOProperties.ACCESS_KEY_ID, "ak");
        properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "sk");
        properties.put(AwsClientProperties.CLIENT_REGION, "xxx");
        factory = new IcebergAwsClientFactory();
        factory.initialize(properties);
        Assertions.assertNotNull(factory.s3());
    }

    @Test
    public void testResolveRegion() {
        Assertions.assertEquals(Region.US_WEST_1, IcebergAwsClientFactory.tryToResolveRegion("us-west-1"));
    }

    @Test
    public void testEnsureSchemeInEndpoint() {
        // test endpoint without scheme
        URI uriWithoutScheme = IcebergAwsClientFactory.ensureSchemeInEndpoint("s3.aa-bbbbb-3.amazonaws.com.cn");
        Assertions.assertEquals("https://s3.aa-bbbbb-3.amazonaws.com.cn", uriWithoutScheme.toString());

        // test endpoint with scheme HTTPS
        URI uriWithHttps = IcebergAwsClientFactory.ensureSchemeInEndpoint("https://s3.aa-bbbbb-3.amazonaws.com.cn");
        Assertions.assertEquals("https://s3.aa-bbbbb-3.amazonaws.com.cn", uriWithHttps.toString());

        // test endpoint with scheme HTTP
        URI uriWithHttp = IcebergAwsClientFactory.ensureSchemeInEndpoint("http://s3.aa-bbbbb-3.amazonaws.com.cn");
        Assertions.assertEquals("http://s3.aa-bbbbb-3.amazonaws.com.cn", uriWithHttp.toString());
    }
}
