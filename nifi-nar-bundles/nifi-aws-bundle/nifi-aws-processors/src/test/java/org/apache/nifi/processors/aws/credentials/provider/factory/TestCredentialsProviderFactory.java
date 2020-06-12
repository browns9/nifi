/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.aws.credentials.provider.factory;

import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;

import org.junit.Assert;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
/**
 * Tests of the validation and credentials provider capabilities of CredentialsProviderFactory.
 */
@SuppressWarnings("deprecation")
public class TestCredentialsProviderFactory {

    /**
     * The {@link TestRunner} to run the test.
     */
    private TestRunner runner = null;

    /**
     * The AWS region name.
     */
    private String awsRegion = null;

    /**
     * Run before each method annotated with {@link Test}.
     */
    @Before
    public void setUp() {

        // The AWS Client builder needs to find a region.
        awsRegion = "us-east-2";

        System.setProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY, awsRegion);

        runner = TestRunners.newTestRunner(MockAWSProcessor.class);
    }

    /**
     * Run after each method annotated with {@link Test} completes.
     *
     * <p>Always runs, regardless of the test method's outcome.</p>
     */
    @After
    public void tearDown() {
        System.clearProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY);
        awsRegion = null;
        runner.shutdown();
        runner = null;
    }

    /**
     * Test implied default credentials.
     */
    @Test
    public void testImpliedDefaultCredentials() {

        doCredentialProviderFactoryTest(DefaultAWSCredentialsProviderChain.class);
    }

    /**
     * Test explicit default credentials.
     */
    @Test
    public void testExplicitDefaultCredentials() {
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "true");

        doCredentialProviderFactoryTest(DefaultAWSCredentialsProviderChain.class);
    }

    /**
     * Test explicit default credentials and using an access key are mutually exclusive.
     */
    @Test
    public void testExplicitDefaultCredentialsExclusive() {
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "true");
        runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        runner.assertNotValid();
    }

    /**
     * Test access key pair credentials.
     */
    @Test
    public void testAccessKeyPairCredentials() {
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "false");
        runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        runner.setProperty(CredentialPropertyDescriptors.SECRET_KEY, "BogusSecretKey");

        doCredentialProviderFactoryTest(StaticCredentialsProvider.class);
    }

    /**
     * Test access key pair credentials with missing secret key.
     */
    @Test
    public void testAccessKeyPairIncomplete() {
        runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        runner.assertNotValid();
    }

    /**
     * Test access key pair credentials with missing secret key.
     */
    @Test
    public void testAccessKeyPairIncompleteS3() {
        final TestRunner s3Runner = TestRunners.newTestRunner(FetchS3Object.class);
        s3Runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        s3Runner.assertNotValid();
    }

    /**
     * Test using a credentials file.
     */
    @Test
    public void testFileCredentials() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE,
                        "src/test/resources/mock-aws-credentials.properties");
        runner.assertValid();

        doCredentialProviderFactoryTest(PropertiesFileCredentialsProvider.class);
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider}.
     */
    @Test
    public void testAssumeRoleCredentials() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE,
                           "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");

        doCredentialProviderFactoryTest(STSAssumeRoleSessionCredentialsProvider.class);
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider} with a missing ARN.
     */
    @Test
    public void testAssumeRoleCredentialsMissingARN() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE,
                           "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.assertNotValid();
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider} with an invalid session length.
     */
    @Test
    public void testAssumeRoleCredentialsInvalidSessionTime() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE,
                           "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.MAX_SESSION_TIME, "10");
        runner.assertNotValid();
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider} an external ID and missing ARN and ARN name.
     */
    @Test
    public void testAssumeRoleExternalIdMissingArnAndName() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE,
                           "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_EXTERNAL_ID, "BogusExternalId");
        runner.assertNotValid();
    }

    /**
     * Test using anonymous credentials.
     */
    @Test
    public void testAnonymousCredentials() {

        runner.setProperty(CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertValid();

        final Map<PropertyDescriptor, String> properties = runner.getProcessContext().getProperties();
        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(properties);
        Assert.assertNotNull(credentialsProvider);

        final AWSCredentials credentials = credentialsProvider.getCredentials();
        final Class<? extends AWSCredentials> actualCredentialsClass = credentials.getClass();
        final Class<?> expectedCredentialsClass = AnonymousAWSCredentials.class;

        Assert.assertTrue("credentials class should be " + expectedCredentialsClass.getName() +
                          "but is " + actualCredentialsClass.getName(),
                          expectedCredentialsClass == actualCredentialsClass);
    }

    /**
     * Test anonymous credentials and default credentials are mutually exclusive.
     */
    @Test
    public void testAnonymousAndDefaultCredentials() {
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "true");
        runner.setProperty(CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertNotValid();
    }

    /**
     * Test using a profile with named credentials.
     */
    @Test
    public void testNamedProfileCredentials() {
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "false");
        runner.setProperty(CredentialPropertyDescriptors.PROFILE_NAME, "BogusProfile");

        doCredentialProviderFactoryTest(ProfileCredentialsProvider.class);
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider} with a proxy.
     */
    @Test
    public void testAssumeRoleCredentialsWithProxy() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE,
                           "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST, "proxy.company.com");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT, "8080");

        doCredentialProviderFactoryTest(STSAssumeRoleSessionCredentialsProvider.class);
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider} with a proxy, but no proxy host.
     */
    @Test
    public void testAssumeRoleMissingProxyHost() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT, "8080");
        runner.assertNotValid();
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider} with a proxy, but no proxy port.
     */
    @Test
    public void testAssumeRoleMissingProxyPort() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST, "proxy.company.com");
        runner.assertNotValid();
    }

    /**
     * Test assuming a role (@link STSAssumeRoleSessionCredentialsProvider} with a proxy, but an invalid proxy port.
     */
    @Test
    public void testAssumeRoleInvalidProxyPort() {
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST, "proxy.company.com");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT, "notIntPort");
        runner.assertNotValid();
    }

    /**
     * Test happy path for Web Identity Token Strategy.
     */
    @Test
    public void testWebIdentityTokenCredentialsHappyPath() {

        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                           "arn:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, "session-role-name");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources/mock-aws-credentials.properties");

        doCredentialProviderFactoryTest(STSAssumeRoleWithWebIdentitySessionCredentialsProvider.class);
    }

    /**
     * Test happy path for Web Identity Token Strategy.
     *
     * <p>The Role Session Name pattern has a NiFi expression.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsRoleSessionNameExpression() {

        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                           "arn:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME,
                           "nifi-${now():format('yyyyMMddHHmmssSSS')}");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources/mock-aws-credentials.properties");

        doCredentialProviderFactoryTest(STSAssumeRoleWithWebIdentitySessionCredentialsProvider.class);
    }

    /**
     * Test web identity role ARN not set for Web Identity Token Strategy.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityRoleArnNotSet() {
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, "session-role-name");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources/mock-aws-credentials.properties");

        runner.assertNotValid();
    }

    /**
     * Test bad web identity role ARN for Web Identity Token Strategy.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityBadRoleArn() {
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                "xxx:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, "session-role-name");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources/mock-aws-credentials.properties");

        runner.assertNotValid();
    }

    /**
     * Test web identity role session name for Web Identity Token Strategy is not set.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityRoleSessionNameNotSet() {
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                           "arn:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources/mock-aws-credentials.properties");

        runner.assertNotValid();
    }

    /**
     * Test Web Identity Token Strategy.
     *
     * <p>The Role Session Name pattern has a bad NiFi expression.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsRoleSessionNameBadExpression() {

        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                           "arn:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME,
                           "nifi-${now(:format('yyyyMMddHHmmssSSS')}");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources/mock-aws-credentials.properties");

        runner.assertNotValid();
    }

    /**
     * Test web identity token file for Web Identity Token Strategy is not set.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityWebTokenFileNotSet() {
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                           "arn:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, "session-role-name");

        runner.assertNotValid();
    }

    /**
     * Test Web Identity Token Strategy.
     *
     * <p>The Role Session Name pattern has a bad NiFi expression.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsTokenFileNotExist() {

        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                           "arn:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME,
                           "nifi-${now(:format('yyyyMMddHHmmssSSS')}");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources/ock-aws-credentials.properties");

        runner.assertNotValid();
    }

    /**
     * Test Web Identity Token Strategy.
     *
     * <p>The Role Session Name pattern has a bad NiFi expression.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsTokenFileNotFile() {

        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN,
                           "arn:partition:service:" + awsRegion + ":account-id:some_resource");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME,
                           "nifi-${now(:format('yyyyMMddHHmmssSSS')}");
        runner.setProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE,
                           "src/test/resources");

        runner.assertNotValid();
    }

    /**
     * Run a Credential Provider Factory test.
     * @param expectedProviderClass
     *          The expected provider implementation class.
     */
    private void doCredentialProviderFactoryTest(final Class<?> expectedProviderClass) {

        runner.assertValid();

        final Map<PropertyDescriptor, String> properties = runner.getProcessContext().getProperties();
        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(properties);

        Assert.assertNotNull(credentialsProvider);

        final Class<? extends AWSCredentialsProvider> actualProviderClass = credentialsProvider.getClass();
        Assert.assertTrue("The credentials provider class should be " + expectedProviderClass.getName() +"," +
                          "but is " + actualProviderClass.getName() + ".",
                          expectedProviderClass == actualProviderClass);
    }
}
