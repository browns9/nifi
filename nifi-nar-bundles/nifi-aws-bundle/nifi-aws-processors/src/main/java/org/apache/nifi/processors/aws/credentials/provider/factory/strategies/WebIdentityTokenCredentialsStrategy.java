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
package org.apache.nifi.processors.aws.credentials.provider.factory.strategies;

import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.util.StringUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryPolicy.RetryCondition;
import com.amazonaws.retry.RetryUtils;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.IDPCommunicationErrorException;
import com.amazonaws.services.securitytoken.model.InvalidIdentityTokenException;


/**
 * Supports AWS credentials via a Web Identity Token.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/WebIdentityTokenCredentialsProvider.html">
 *     AWS WebIdentityTokenCredentialsProvider</a>
 * @see <a href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/WebIdentityTokenCredentialsProvider.Builder.html">
 *     AWS WebIdentityTokenCredentialsProvider Builder</a>
 */
public class WebIdentityTokenCredentialsStrategy extends AbstractAssumeRoleCredentialsStrategy {

    /**
     * Class to determine whether or not an STS request should be retried.
     * @author Steve Brown, Estafet Ltd.
     *
     */
    private static class SecureTokenServiceRetryCondition implements RetryCondition {

        /**
         * Determine if failed request should be retried, according to the given request context.
         *
         * <p>A request will fail directly, without calling this method, in these circumstances:</p>
         * <ul>
         *   <li> if it has already reached the max retry limit,</li>
         *   <li> if the request contains non-repeatable content,</li>
         *   <li> if any RuntimeException or Error is thrown when executing the request.</li>
         * </ul>
         *
         * @param originalRequest
         *          The original request object being executed.
         * @param exception
         *          The exception from the failed request, represented as an {@link AmazonClientException} object.
         *          There are two types of exception that will be passed to this method:
         *            <ul>
         *            <li>An {@link AmazonServiceException}, indicating a service error.</li>
         *            <li>An {@link AmazonClientException} caused by an {@link IOException} when executing the HTTP
         *                request.</li>
         *            </ul>
         *          All other exceptions are regarded as an unexpected failure, and are thrown immediately without any
         *          retry.
         * @param retriesAttempted
         *          The number of times the current request has been attempted.
         *
         * @return
         *          {@code true} if the failed request should be retried.
         */
        @Override
        public boolean shouldRetry(final AmazonWebServiceRequest originalRequest,
                                   final AmazonClientException exception,
                                   int retriesAttempted) {
            // Always retry on client exceptions caused by IOException
            if (exception.getCause() instanceof IOException) {
                return true;
            }

            if (exception.getCause() instanceof InvalidIdentityTokenException) {
                return true;
            }

            if (exception.getCause() instanceof IDPCommunicationErrorException) {
                return true;
            }

            // Only retry on a subset of service exceptions
            if (exception instanceof AmazonServiceException) {
                AmazonServiceException ase = (AmazonServiceException)exception;

                // For 500 internal server errors and 503 service unavailable errors, we want to retry, but we need
                // to use an exponential back-off strategy so that we don't overload a server with a flood of retries.
                if (RetryUtils.isRetryableServiceException(ase)) {
                    return true;
                }

                // Throttling is reported as a 400 error from newer services. To try and smooth out an occasional
                // throttling error, we'll pause and retry, hoping that the pause is long enough for the request to
                // get through the next time.
                if (RetryUtils.isThrottlingException(ase)) {
                    return true;
                }

                // Clock skew exception. If it is, then we will get the time offset between the device time and the
                // server time to set the clock skew, and then retry the request.
                if (RetryUtils.isClockSkewError(ase)) {
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * The maximum number of retries for an STS request.
     */
    private static final int MAXIMUM_RETRIES = 3;

    /**
     * Construct from a strategy name and a list of properties.
     */
    public WebIdentityTokenCredentialsStrategy() {
        super("Web Identity Token", new PropertyDescriptor[] {
                WEB_IDENTITY_ROLE_ARN,
                WEB_IDENTITY_ROLE_SESSION_NAME,
                WEB_IDENTITY_TOKEN_FILE
        });
    }

    /**
     * Determines if this strategy can create derived credentials using the given properties.
     *
     * <p>A derived credential can be created only if all these properties exist:</p>
     * <ul>
     *      <li>The ARN of the role to be assumed.</li>
     *      <li>The Web Identity Role Session Name</li>
     *      <li>The path to the Web Identity Token File</li>
     * </ul>
     * @return
     *      {@code true} if derived credentials can be created.
     */
    @Override
    public boolean canCreateDerivedCredential(final Map<PropertyDescriptor, String> properties) {
        final String webIdentityRoleArn = properties.get(WEB_IDENTITY_ROLE_ARN);
        if (StringUtils.isEmpty(webIdentityRoleArn)) {
            return false;
        }

        final String webIdentityTokenFile = properties.get(WEB_IDENTITY_TOKEN_FILE);
        if (StringUtils.isEmpty(webIdentityTokenFile)) {
            return false;
        }

        return true;
    }

    /**
     * This strategy cannot create primary credentials from the list of {@link PropertyDescriptor}s.
     * @return Always returns {@code false}.
     */
    @Override
    public boolean canCreatePrimaryCredential(final Map<PropertyDescriptor, String> properties) {
        return false;
    }

    /**
     * Get the AWS Credentials provider.
     *
     * @return  Never returns.
     * @throws UnsupportedOperationException
     *          Because this operation is unsupported.
     */
    @Override
    public AWSCredentialsProvider getCredentialsProvider(final Map<PropertyDescriptor, String> properties) {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates an AWSCredentialsProvider instance for this strategy, given the properties defined by the user.
     *
     * <p>The {@link STSAssumeRoleWithWebIdentitySessionCredentialsProvider} requires anonymous credentials for the
     * Secure Token Service client.</p>
     *
     * @param properties
     *          The properties the properties to use to create the credentials provider.
     * @param primaryCredentialsProvider
     *          The AWS Credentials Provider.
     */
    @Override
    public AWSCredentialsProvider getDerivedCredentialsProvider(
                                        final Map<PropertyDescriptor, String> properties,
                                        final AWSCredentialsProvider primaryCredentialsProvider) {

        final AWSSecurityTokenService securityTokenServiceClient = buildSecurityTokenServiceClient(properties);


        // Build the Assume Role With Web Identity Session Credentials Provider
        final String roleArn = properties.get(WEB_IDENTITY_ROLE_ARN);
        final String roleSessionName = makeRoleSessionName(properties);
        final String webIdentityTokenFile = properties.get(WEB_IDENTITY_TOKEN_FILE);

        final STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder providerBuilder =
                new STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder(roleArn,
                                                                                   roleSessionName,
                                                                                   webIdentityTokenFile)
                                                                          .withStsClient(securityTokenServiceClient);

        final AWSCredentialsProvider credentialsProvider = providerBuilder.build();

        return credentialsProvider;
    }

    /**
     * Create a role session name.
     *
     * <p>If the {@code WEB_IDENTITY_ROLE_SESSION_NAME} property is not set, creates a role session name of the format
     * {@code nifi-<timestamp>}, where timestamp is the number of milliseconds since the start of the epoch.
     * @param properties
     *          The provider properties.
     * @return
     *          A role session name.
     */
    private String makeRoleSessionName(final Map<PropertyDescriptor, String> properties) {
        String roleSessionName = properties.get(WEB_IDENTITY_ROLE_SESSION_NAME);

        if (StringUtils.isEmpty(roleSessionName)) {
            roleSessionName = "nifi-" + Long.toString(System.currentTimeMillis());
        }
        return roleSessionName;
    }

    /**
     * Build the Secure Token Service client.
     * @param properties
     *          The current configuration.
     * @return
     *          The Secure Token Service client as an {@link AWSSecurityTokenService} object.
     */
    private AWSSecurityTokenService buildSecurityTokenServiceClient(final Map<PropertyDescriptor, String> properties) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        addProxyConfiguration(properties, clientConfiguration);

        // The number of retries is the maximum error retry count set by ClientConfiguration.setMaxErrorRetry(int) if
        // that has been set. Otherwise it is MAXIMUM_RETRIES.
        final RetryPolicy retryPolicy = new RetryPolicy(new SecureTokenServiceRetryCondition(),
                                                        new PredefinedBackoffStrategies.SDKDefaultBackoffStrategy(),
                                                        MAXIMUM_RETRIES,
                                                        true);

        clientConfiguration.setRetryPolicy(retryPolicy);

        // Build the Secure Token Service client.
        final AnonymousAWSCredentials credentials = new AnonymousAWSCredentials();
        final AWSStaticCredentialsProvider clientCredentialsProvider = new AWSStaticCredentialsProvider(credentials);
        final AWSSecurityTokenService securityTokenServiceClient =
                AWSSecurityTokenServiceClientBuilder.standard()
                                                    .withClientConfiguration(clientConfiguration)
                                                    .withCredentials(clientCredentialsProvider)
                                                    .build();
        return securityTokenServiceClient;
    }

    /**
     * Validates the properties belonging to this strategy, given the selected primary strategy.
     *
     * <p>Called from {@link org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory#validate(ValidationContext)},
     * which calls the {@code validate(ValidationContext)} method on each of the strategy classes in the
     * {@code org.apache.nifi.processors.aws.credentials.provider.factory.strategies} package, in turn.</p>
     * <p>The {@link org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory#validate(ValidationContext)}
     * method is called from the controller service method
     * {@code org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService#customValidate(ValidationContext)} method.</p>
     *
     * <p>Errors may result from individually malformed properties, invalid combinations of properties, or
     *  inappropriate use of properties not consistent with the primary strategy.</p>
     *
     * @param primaryStrategy
     *          The prevailing primary strategy.
     * @return
     *          Validation errors as a {@link Collection} of {@link ValidationResult} objects.
     */
    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        final Collection<ValidationResult> validationFailureResults = new ArrayList<>();

        validateAllRequiredPropertiesAreSet(validationContext, validationFailureResults);

        super.validateProxyConfiguration(validationContext, validationFailureResults);

        return validationFailureResults.isEmpty() ? null : validationFailureResults;
    }

    /**
     * Create the explanation message used when one or more required properties are not set.
     * @param validationContext
     *          The {@link ValidationContext} to use.
     * @return
     *          The explanation message.
     */
    private String makeExplanationForMissingProperties(final ValidationContext validationContext) {

        final StringBuilder builder = new StringBuilder(2048);

        final String lineEnding = System.getProperty("line.separator");
        builder.append("assuming roles with web identity session credentials requires all these properties to be set:")
               .append(lineEnding);

        for (final PropertyDescriptor requiredProperty : requiredProperties) {
             final boolean isSet = validationContext.getProperty(requiredProperty).isSet();

            if (!isSet) {
                builder.append("The \"")
                       .append(requiredProperty.getDisplayName())
                       .append("\" property must be set.")
                       .append(lineEnding);
            }
        }
        final String explanation = builder.toString();
        return explanation;
    }

    /**
     * Validate that all the required properties are set.
     * @param validationContext
     *          The {@link ValidationContext} to use.
     * @param validationFailureResults
     *          The {@link Collection} of {@link ValidationResult}s for failed validations.
     */
    private void validateAllRequiredPropertiesAreSet(final ValidationContext validationContext,
                                                     final Collection<ValidationResult> validationFailureResults) {
        final boolean webIdentityRoleArnPropertyIsSet = validationContext.getProperty(WEB_IDENTITY_ROLE_ARN).isSet();
        final boolean webIdentityTokenFileIsSet = validationContext.getProperty(WEB_IDENTITY_TOKEN_FILE).isSet();

        // Either none of these properties must be set, or all of them must be set.
        //
        // If all these properties are set, this strategy will be chosen to create the credential provider service.
        // If none of these properties are set, some other strategy will be chosen to create the credential provider
        // service.
        // If some of these properties are set, validation fails.
        if (webIdentityRoleArnPropertyIsSet ^ webIdentityTokenFileIsSet) {
            final String explanation = makeExplanationForMissingProperties(validationContext);

            final ValidationResult validatePropertiesSetResult =
                    new ValidationResult.Builder().subject(super.name)
                                                  .input(null)
                                                  .valid(false)
                                                  .explanation(explanation)
                                                  .build();
            validationFailureResults.add(validatePropertiesSetResult);
        }
    }
}
