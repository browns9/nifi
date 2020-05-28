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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.util.StringUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;


/**
 * Supports AWS credentials via a Web Identity Token.  Assume Role is a derived credential strategy, requiring a primary
 * credential to retrieve and periodically refresh temporary credentials.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/WebIdentityTokenCredentialsProvider.html">
 *     AWS WebIdentityTokenCredentialsProvider</a>
 * @see <a href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/WebIdentityTokenCredentialsProvider.Builder.html">
 *     AWS WebIdentityTokenCredentialsProvider Builder</a>
 * @see <a href=""></a>
 */
public class WebIdentityTokenCredentialsStrategy extends AbstractCredentialsStrategy {


    /**
     * Construct from a name and a list of properties.
     */
    public WebIdentityTokenCredentialsStrategy() {
        super("Web Identity Token", new PropertyDescriptor[] {
                WEB_IDENTITY_ROLE_ARN,
                WEB_IDENTITY_ROLE_SESSION_NAME,
                WEB_IDENTITY_TOKEN_FILE
        });
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

        final String webIdentityRoleSessionName = properties.get(WEB_IDENTITY_ROLE_SESSION_NAME);
        if (StringUtils.isEmpty(webIdentityRoleSessionName)) {
            return false;
        }

        final String webIdentityTokenFile = properties.get(WEB_IDENTITY_TOKEN_FILE);
        if (StringUtils.isEmpty(webIdentityTokenFile)) {
            return false;
        }

        return true;
    }


    /**
     * Validates the properties belonging to this strategy, given the selected primary strategy.
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

        final boolean webIdentityRoleArnPropertyIsSet = validationContext.getProperty(WEB_IDENTITY_ROLE_ARN).isSet();
        final boolean webIdentityRoleSessionNameIsSet =
                validationContext.getProperty(WEB_IDENTITY_ROLE_SESSION_NAME).isSet();
        final boolean webIdentityTokenFileIsSet = validationContext.getProperty(WEB_IDENTITY_TOKEN_FILE).isSet();

        if (webIdentityRoleArnPropertyIsSet ^ webIdentityRoleSessionNameIsSet
            || webIdentityRoleArnPropertyIsSet ^ webIdentityTokenFileIsSet) {
            final String webIdentityRoleArnPropertyName = WEB_IDENTITY_ROLE_ARN.getName();
            final String webIdentityRoleSessionNamePropertyName = WEB_IDENTITY_ROLE_SESSION_NAME.getName();
            final String webIdentityTokeFilePropertyName = WEB_IDENTITY_TOKEN_FILE.getName();
            final ValidationResult validatePropertiesSetResult =
                    new ValidationResult.Builder().input("The " + webIdentityRoleArnPropertyName + ", " +
                                                         webIdentityRoleSessionNamePropertyName + ", and the "  +
                                                         webIdentityTokeFilePropertyName + " properties")
                                                  .valid(false)
                                                  .explanation(" assuming roles with web identity session credentials" +
                                                               " requires all these properties to be set. " +
                                                               "The " + webIdentityRoleArnPropertyName + " property " +
                                                               (webIdentityRoleArnPropertyIsSet ? "is" : "is not") +
                                                               " set. " +
                                                               "The " + webIdentityRoleSessionNamePropertyName +
                                                               " property " +
                                                               (webIdentityRoleSessionNameIsSet ? "is" : "is not") +
                                                               " set. " +
                                                               "The " + webIdentityTokeFilePropertyName + " property " +
                                                               (webIdentityTokenFileIsSet ? "is" : "is not") +
                                                               " set.")
                                                  .build();
            validationFailureResults.add(validatePropertiesSetResult);
        }

        return validationFailureResults.isEmpty() ? null : validationFailureResults;
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
     * Creates an AWSCredentialsProvider instance for this strategy, given the properties defined by the user and
     * the AWSCredentialsProvider from the winning primary strategy.
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

        final ClientConfiguration clientConfiguration = new ClientConfiguration();

        final AWSSecurityTokenServiceClientBuilder clientBuilder =
                AWSSecurityTokenServiceClientBuilder.standard().withCredentials(primaryCredentialsProvider)
                                                               .withClientConfiguration(clientConfiguration);


        final String roleArn = properties.get(WEB_IDENTITY_ROLE_ARN);
        final String roleSessionName = properties.get(WEB_IDENTITY_ROLE_SESSION_NAME);
        final String webIdentityTokenFile = properties.get(WEB_IDENTITY_TOKEN_FILE);
        final AWSSecurityTokenService securityTokenService = clientBuilder.build();
        final STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder providerBuilder =
                new STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder(roleArn,
                                                                                   roleSessionName,
                                                                                   webIdentityTokenFile)
                                                                          .withStsClient(securityTokenService);

        final AWSCredentialsProvider credentialsProvider = providerBuilder.build();

        return credentialsProvider;
    }
}
