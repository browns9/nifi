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
package org.apache.nifi.processors.aws.credentials.provider.service;

import static org.junit.Assert.assertEquals;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.AbstractAWSProcessor;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;


/**
 * Test suite for {@link AWSCredentialsProviderControllerService}.
 *
 */
@SuppressWarnings("deprecation")
public class AWSCredentialsProviderControllerServiceTest {

    /**
     * A web identity token file that doesn't exist.
     */
    private static final String NON_EXISTENT_TOKEN_FILE = "src/test/resources/ock-aws-credentials.properties";

    /**
     * A dummy Session role name.
     */
    private static final String DUMMY_SESSION_ROLE_NAME = "session-role-name";

    /**
     * An invalid NiFi expression
     */
    private static final String INVALID_NIFI_EXPRESSION = "nifi-${now(:format('yyyyMMddHHmmssSSS')}";

    /**
     * A value for the Web Identity Token File that is a directory, not a regular file.
     */
    private static final String WEB_TOKEN_FILE_IS_DIRECTORY = "src/test/resources";

    /**
     * A valid NiFi expression for the Wed Identity Role Session name.
     */
    private static final String VALID_NIFI_EXPRESSION = "nifi-${now():format('yyyyMMddHHmmssSSS')}";

    /**
     * Invalid path to a credentials file.
     */
    private static final String INVALID_CREDENTIALS_FILE = "src/test/resources/bad-mock-aws-credentials.properties";

    /**
     * Valid path to a credentials file.
     */
    private static final String VALID_CREDENTENTIALS_FILE = "src/test/resources/mock-aws-credentials.properties";

    /**
     * Session timeout value that is not an integer.
     */
    private static final String INVALID_TIMEOUT = "xyz";

    /**
     * Session timeout value that is too long.
     */
    private static final String TOO_LONG_TIMEOUT = "3601";

    /**
     * Session timeout value that is too short.
     */
    private static final String TOO_SHORT_TIMEOUT = "899";

    /**
     * Maximum valid session timeout value.
     */
    private static final String MAXIMUM_TIMEOUT = "3600";

    /**
     * Minimum valid session timeout value.
     */
    private static final String MINIMUM_TIMEOUT = "900";

    /**
     * Session timeout value that is between {@link #MINIMUM_TIMEOUT} and {@link #MAXIMUM_TIMEOUT}.
     */
    private static final String VALID_TIMEOUT = "1000";

    /**
     * A role name.
     */
    private static final String ROLE_NAME = "RoleName";

    /**
     * A role ARN.
     */
    private static final String ROLE_ARN = "Role";

    /**
     * The value of an AWS secret key property.
     */
    private static final String AWS_SECRET_KEY_VALUE = "awsSecretKey";

    /**
     * The value of an AWS access key property.
     */
    private static final String AWS_ACCESS_KEY_VALUE = "awsAccessKey";

    /**
     * The Credentials Provider Id.
     */
    private static final String CREDENTIALS_PROVIDER_ID = "awsCredentialsProvider";

    /**
     * The AWS region name.
     * <p>The AWS Client builder needs to find a region.</p>
     */
    private static final String AWS_REGION = "us-east-2";

    /**
     * An invalid role ARN (doesn't start with "arn:").
     */
    private static final String INVALID_ROLE_ARN = "xxx:partition:service:" + AWS_REGION + ":account-id:some_resource";

    /**
     * A valid Role ARN.
     */
    private static final String VALID_ROLE_ARN = "arn:partition:service:" + AWS_REGION + ":account-id:some_resource";

    /**
     * The test runner to use.
     */
    private TestRunner runner = null;

    /**
     * The credentials provider controller service implementation.
     */
    private AWSCredentialsProviderControllerService serviceImpl = null;

    /**
     * The set of properties for each test method.
     */
    private TestPropertySet testPropertySet = null;

    /**
     * Run before each test method is executed.
     * @throws Exception
     *          If an error occurs.
     */
    @Before
    public void setUp() throws Exception {

        System.setProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY, AWS_REGION);

        runner = TestRunners.newTestRunner(FetchS3Object.class);

        serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService(CREDENTIALS_PROVIDER_ID, serviceImpl);

        testPropertySet = new TestPropertySet(serviceImpl);
    }

    /**
     * Run after each test method completes.
     *
     * <p>Always runs, regardless of the test outcome.</p>
     */
    @After
    public void tearDown() {

        if (runner != null) {
            runner.shutdown();
            runner = null;
        }

        serviceImpl = null;

        testPropertySet = null;
    }

    /**
     * Test the default AWS credentials provider chain.
     */
    @Test
    public void testDefaultAWSCredentialsProviderChain() {
        doServiceTest(DefaultAWSCredentialsProviderChain.class);
    }

    /**
     * Test with an AWS access and AWS secret key.
     */
    @Test
    public void testKeysCredentialsProvider() {
        testPropertySet.addProperty(AbstractAWSProcessor.ACCESS_KEY, AWS_ACCESS_KEY_VALUE)
                       .addProperty(AbstractAWSProcessor.SECRET_KEY, AWS_SECRET_KEY_VALUE);

        doServiceTest(StaticCredentialsProvider.class);
    }

    /**
     * Test Assume Role.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleAndName() {
        testPropertySet.addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN)
                       .addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME);

        doServiceTest(STSAssumeRoleSessionCredentialsProvider.class);
    }

    /**
     * Test assume role with a valid session timeout value.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleAndNameAndSessionTimeoutInRange() {
        testPropertySet.addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN)
                       .addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME)
                       .addProperty(AWSCredentialsProviderControllerService.MAX_SESSION_TIME, VALID_TIMEOUT);

        doServiceTest(STSAssumeRoleSessionCredentialsProvider.class);
    }

    /**
     * Test assume role with the minimum session timeout value.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleAndNameAndSessionTimeout900() {
        testPropertySet.addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN)
                       .addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME)
                       .addProperty(AWSCredentialsProviderControllerService.MAX_SESSION_TIME, MINIMUM_TIMEOUT);

        doServiceTest(STSAssumeRoleSessionCredentialsProvider.class);
    }

    /**
     * Test assume role with an invalid session timeout value.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleAndNameAndSessionTimeoutNotANumber() {
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN);
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME);
        setInvalidProperty(AWSCredentialsProviderControllerService.MAX_SESSION_TIME, INVALID_TIMEOUT);
        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test assume role with the maximum session timeout value.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleAndNameAndMaximumSessionTimeout() {
        testPropertySet.addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN)
                       .addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME)
                       .addProperty(AWSCredentialsProviderControllerService.MAX_SESSION_TIME, MAXIMUM_TIMEOUT);

        doServiceTest(STSAssumeRoleSessionCredentialsProvider.class);
    }

    /**
     * Test assume role with a session timeout value less than the minimum session timeout value.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleAndNameAndSessionTimeoutTooShort() {
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN);
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME);
        setValidProperty(AWSCredentialsProviderControllerService.MAX_SESSION_TIME, TOO_SHORT_TIMEOUT);
        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test assume role with a session timeout value longer than the maximum session timeout value.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleAndNameAndSessionTimeoutTooLong() {
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN);
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME);
        setValidProperty(AWSCredentialsProviderControllerService.MAX_SESSION_TIME, TOO_LONG_TIMEOUT);
        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test assume role with a missing role name.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleOnlyInvalid() {
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test assume role with a missing role ARN.
     */
    @Test
    public void testAssumeRoleCredentialsProviderWithRoleNameOnlyInvalid() {
        setValidProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test assume role with a credentials file.
     */
    @Test
    public void testAssumeRoleWithCredentialsFile() {
        testPropertySet.addProperty(AbstractAWSProcessor.CREDENTIALS_FILE, VALID_CREDENTENTIALS_FILE)
                       .addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, ROLE_ARN)
                       .addProperty(AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, ROLE_NAME);

        doServiceTest(STSAssumeRoleSessionCredentialsProvider.class);
    }

    /**
     * Test with a properties file credentials provider.
     */
    @Test
    public void testFileCredentialsProvider() {
        testPropertySet.addProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, VALID_CREDENTENTIALS_FILE);

        doServiceTest(PropertiesFileCredentialsProvider.class);
    }

    /**
     * Test a file credentials provider with a non-existent credentials file.
     */
    @Test
    public void testFileCredentialsProviderBadFile() {
        setInvalidProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, INVALID_CREDENTIALS_FILE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test using a credentials file and AWS access and secret keys are mutually exclusive.
     */
    @Test
    public void testFileAndAccessSecretKeyInvalid() {
        setValidProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, VALID_CREDENTENTIALS_FILE);
        setValidProperty(CredentialPropertyDescriptors.ACCESS_KEY, AWS_ACCESS_KEY_VALUE);
        setValidProperty(CredentialPropertyDescriptors.SECRET_KEY, AWS_SECRET_KEY_VALUE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test using a credentials file and an AWS access key is mutually exclusive.
     */
    @Test
    public void testFileAndAccessKeyInvalid() {
        setValidProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, VALID_CREDENTENTIALS_FILE);
        setValidProperty(CredentialPropertyDescriptors.ACCESS_KEY, AWS_ACCESS_KEY_VALUE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test using a credentials file and an AWS secret key is mutually exclusive.
     */
    @Test
    public void testFileAndSecretKeyInvalid() {
        setValidProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, VALID_CREDENTENTIALS_FILE);
        setValidProperty(CredentialPropertyDescriptors.SECRET_KEY, AWS_SECRET_KEY_VALUE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test using a only an AWS access key is invalid.
     */
    @Test
    public void testAccessKeyOnlyInvalid() {
        setValidProperty(CredentialPropertyDescriptors.ACCESS_KEY, AWS_ACCESS_KEY_VALUE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test using a only an AWS secret key is invalid.
     */
    @Test
    public void testSecretKeyOnlyInvalid() {
        setValidProperty(CredentialPropertyDescriptors.SECRET_KEY, AWS_SECRET_KEY_VALUE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test the access key and secret key properties support NiFi expressions.
     */
    @Test
    public void testExpressionLanguageSupport() {

        final String expectedAccessKeyValue = AWS_ACCESS_KEY_VALUE;
        final String accessKeyNiFiExpression = "${literal(\"" + expectedAccessKeyValue + "\")}";
        testPropertySet.addProperty(CredentialPropertyDescriptors.ACCESS_KEY, accessKeyNiFiExpression);

        final String expectedSecretKeyValue = AWS_SECRET_KEY_VALUE;
        final String secretKeyNiFiExpression = "${literal(\"" + expectedSecretKeyValue + "\")}";
        testPropertySet.addProperty(CredentialPropertyDescriptors.SECRET_KEY, secretKeyNiFiExpression);

        testPropertySet.setAllProperties(runner).assertAllPropertiesAreValid();
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final AWSCredentialsProviderService service =
                (AWSCredentialsProviderService) runner.getProcessContext()
                                                      .getControllerServiceLookup()
                                                      .getControllerService(CREDENTIALS_PROVIDER_ID);

        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        final AWSCredentials credentials = credentialsProvider.getCredentials();

        final String actualAccessKeyValue = credentials.getAWSAccessKeyId();
        final String accessKeyPropertyName = CredentialPropertyDescriptors.ACCESS_KEY.getName();
        assertEquals("The NiFi expression \"" + accessKeyNiFiExpression + "\" was not correctly evaluated for the " +
                     accessKeyPropertyName + " property.\n" +
                     "The expected value was \"" + expectedAccessKeyValue + "\", " +
                     "but the actual value was \"" + actualAccessKeyValue + "\".",
                     expectedAccessKeyValue,
                     actualAccessKeyValue);

        final String actualSecretKeyValue = credentials.getAWSSecretKey();
        final String secretKeyPropertyName = CredentialPropertyDescriptors.SECRET_KEY.getName();
        assertEquals("The NiFi expression \"" + secretKeyNiFiExpression + "\" was not correctly evaluated for the " +
                     secretKeyPropertyName + " property.\n" +
                     "The expected value was \"" + expectedSecretKeyValue + "\", " +
                     "but the actual value was \"" + actualSecretKeyValue + "\".",
                     expectedSecretKeyValue,
                     actualSecretKeyValue);
    }

    /**
     * Test happy path for Web Identity Token Strategy.
     */
    @Test
    public void testWebIdentityTokenCredentialsHappyPath() {

        final String webIdentityRoleArn = VALID_ROLE_ARN;
        final String webIdentityRoleSessionName = DUMMY_SESSION_ROLE_NAME;
        final String webIdentityTokenFile = VALID_CREDENTENTIALS_FILE;

        testPropertySet.addProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, webIdentityRoleArn)
                       .addProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME,
                                    webIdentityRoleSessionName)
                       .addProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, webIdentityTokenFile);

        doServiceTest(STSAssumeRoleWithWebIdentitySessionCredentialsProvider.class);
    }

    /**
     * Test happy path for Web Identity Token Strategy.
     *
     * <p>The Role Session Name pattern has a NiFi expression.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsRoleSessionNameExpression() {

        final String webIdentityRoleArn = VALID_ROLE_ARN;
        final String webIdentityRoleSessionName = VALID_NIFI_EXPRESSION;
        final String webIdentityTokenFile = VALID_CREDENTENTIALS_FILE;

        testPropertySet.addProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, webIdentityRoleArn)
                       .addProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME,
                                    webIdentityRoleSessionName)
                       .addProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, webIdentityTokenFile);

        doServiceTest(STSAssumeRoleWithWebIdentitySessionCredentialsProvider.class);
    }

    /**
     * Test web identity role session name for Web Identity Token Strategy is not set.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityRoleSessionNameNotSet() {
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, VALID_ROLE_ARN);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, VALID_CREDENTENTIALS_FILE);

        doServiceTest(STSAssumeRoleWithWebIdentitySessionCredentialsProvider.class);
    }

    /**
     * Test web identity role ARN not set for Web Identity Token Strategy.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityRoleArnNotSet() {
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, DUMMY_SESSION_ROLE_NAME);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, VALID_CREDENTENTIALS_FILE);

        runner.assertNotValid();
    }

    /**
     * Test bad web identity role ARN for Web Identity Token Strategy.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityBadRoleArn() {
        setInvalidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, INVALID_ROLE_ARN);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, DUMMY_SESSION_ROLE_NAME);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, VALID_CREDENTENTIALS_FILE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test Web Identity Token Strategy.
     *
     * <p>The Role Session Name pattern has a bad NiFi expression.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsRoleSessionNameBadExpression() {

        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, VALID_ROLE_ARN);
        setInvalidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, INVALID_NIFI_EXPRESSION);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, VALID_CREDENTENTIALS_FILE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test web identity token file not set.
     */
    @Test
    public void testWebIdentityTokenCredentialsWebIdentityWebTokenFileNotSet() {
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, VALID_ROLE_ARN);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, DUMMY_SESSION_ROLE_NAME);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test Web Identity Token File.
     *
     * <p>The Web Identity Token File path is not valid.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsTokenFileNotExist() {

        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, VALID_ROLE_ARN);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, VALID_NIFI_EXPRESSION);
        setInvalidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, NON_EXISTENT_TOKEN_FILE);

        runner.assertNotValid(serviceImpl);
    }

    /**
     * Test Web Identity Token.
     *
     * <p>The Web Identity Token file path is not a path to a file.</p>
     */
    @Test
    public void testWebIdentityTokenCredentialsTokenFileNotFile() {

        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN, VALID_ROLE_ARN);
        setValidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME, VALID_NIFI_EXPRESSION);
        setInvalidProperty(CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE, WEB_TOKEN_FILE_IS_DIRECTORY);

        runner.assertNotValid(serviceImpl);
    }


    /**
     * Execute a service test.
     * @param expectedProviderClass
     *          The expected class or the credentials provider.
     * @throws ProcessException
     *          If an error occurs in {@code service.getCredentialsProvider()}
     */
    private void doServiceTest(final Class<?> expectedProviderClass) throws ProcessException {

        testPropertySet.setAllProperties(runner)
                          .assertAllPropertiesAreValid();

        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final ControllerService controllerService = runner.getProcessContext()
                                                          .getControllerServiceLookup()
                                                          .getControllerService(CREDENTIALS_PROVIDER_ID);
        Assert.assertNotNull(controllerService);

        final AWSCredentialsProviderControllerService service =
                                                (AWSCredentialsProviderControllerService)controllerService;
        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        Assert.assertNotNull(credentialsProvider);

        final Class<? extends AWSCredentialsProvider> actualProviderClass = credentialsProvider.getClass();

        Assert.assertTrue("The credentials provider class should be " + expectedProviderClass.getName() +"," +
                          "but is " + actualProviderClass.getName() + ".",
                          expectedProviderClass == actualProviderClass);
    }

    /**
     * Set a property and verify it validates OK.
     * @param descriptor
     *          The property descriptor.
     * @param value
     *          The property value.
     */
    private void setValidProperty(final PropertyDescriptor descriptor, final String value) {
        setAndValidateProperty(descriptor, value, true);
    }

    /**
     * Set a property and verify it does not validate.
     * @param descriptor
     *          The property descriptor.
     * @param value
     *          The property value.
     */
    private void setInvalidProperty(final PropertyDescriptor descriptor, final String value) {
        setAndValidateProperty(descriptor, value, false);
    }


    /**
     * Set a property and verify validating it has the expected result.
     * @param descriptor
     *          The property descriptor.
     * @param value
     *          The property value.
     * @param expectedResult
     *          {@code true} if the property is expected to valid. {@code false} otherwise.
     */
    private void setAndValidateProperty(final PropertyDescriptor descriptor,
                                        final String value,
                                        final boolean expectedResult) {
        final ValidationResult validationResult = runner.setProperty(serviceImpl, descriptor, value);

        final boolean actualResult = validationResult.isValid();

        final StringBuilder builder = new StringBuilder(1024);

        builder.append("Setting the ")
               .append(descriptor.toString())
               .append(" should have ").append(expectedResult ? "succeeded" : "failed").append(".\n")
               .append("Instead, it ").append(actualResult ? "succeeded" : "failed").append(".\n")
               .append(validationResult.toString()).append("\n");
        Assert.assertTrue(builder.toString(), expectedResult == actualResult);
    }

}
