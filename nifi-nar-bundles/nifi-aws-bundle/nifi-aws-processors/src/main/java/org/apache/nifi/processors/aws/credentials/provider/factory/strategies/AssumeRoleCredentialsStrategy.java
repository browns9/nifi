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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.exception.ProcessException;

import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.MAX_SESSION_TIME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_NAME;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;


/**
 * Supports AWS credentials via Assume Role.  Assume Role is a derived credential strategy, requiring a primary
 * credential to retrieve and periodically refresh temporary credentials.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html">
 *     STSAssumeRoleCredentialsProvider</a>
 */
public class AssumeRoleCredentialsStrategy extends AbstractAssumeRoleCredentialsStrategy {

    /**
     * The maximum session length in seconds.
     */
    private static final int MAXIMUM_SESSION_LENGTH = 3_600;

    /**
     * The minimum session length in seconds.
     */
    private static final int MINIMUM_SESSION_LENGTH = 900;

    /**
     * Construct from a strategy name and a list of properties.
     */
    public AssumeRoleCredentialsStrategy() {
        super("Assume Role", new PropertyDescriptor[] {
                ASSUME_ROLE_ARN,
                ASSUME_ROLE_NAME,
                MAX_SESSION_TIME,
        });
    }

    /**
     * This strategy cannot create primary credentials from the list of {@link PropertyDescriptor}s.
     * @return Always returns {@code false}.
     */
    @Override
    public boolean canCreatePrimaryCredential(Map<PropertyDescriptor, String> properties) {
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
    public boolean canCreateDerivedCredential(Map<PropertyDescriptor, String> properties) {
        final String assumeRoleArn = properties.get(ASSUME_ROLE_ARN);
        final String assumeRoleName = properties.get(ASSUME_ROLE_NAME);
        if (StringUtils.isEmpty(assumeRoleArn) || StringUtils.isEmpty(assumeRoleName)) {
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

        final Collection<ValidationResult> validationFailureResults  = new ArrayList<>();

        // Both role and arn name are req if present
        final boolean assumeRoleArnIsSet = validationContext.getProperty(ASSUME_ROLE_ARN).isSet();
        final boolean assumeRoleNameIsSet = validationContext.getProperty(ASSUME_ROLE_NAME).isSet();
        if (assumeRoleArnIsSet ^ assumeRoleNameIsSet ) {
            validationFailureResults.add(
                    new ValidationResult.Builder().input("Assume Role Arn and Name")
                                                  .valid(false)
                                                  .explanation("Assume role requires both arn and name to be set")
                                                  .build());
        }

        validateSessionLength(validationFailureResults, validationContext, MAX_SESSION_TIME);

        // External ID should only be provided with viable Assume Role ARN and Name
        final boolean assumeRoleExternalIdIsSet = validationContext.getProperty(ASSUME_ROLE_EXTERNAL_ID).isSet();
        if (assumeRoleExternalIdIsSet && (!assumeRoleArnIsSet || !assumeRoleNameIsSet)) {
            validationFailureResults.add(
                    new ValidationResult.Builder().input("Assume Role External ID")
                                                  .valid(false)
                                                  .explanation("Assume role requires both arn and name to be set " +
                                                               "with External ID")
                                                  .build());
        }

        super.validateProxyConfiguration(validationContext, validationFailureResults);

        return validationFailureResults;
    }

    /**
     * Get the AWS Credentials provider.
     *
     * @return  Never returns.
     * @throws UnsupportedOperationException
     *          Because this operation is unsupported.
     */
    @Override
    public AWSCredentialsProvider getCredentialsProvider(Map<PropertyDescriptor, String> properties) {
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
    public AWSCredentialsProvider getDerivedCredentialsProvider(Map<PropertyDescriptor, String> properties,
                                                                AWSCredentialsProvider primaryCredentialsProvider) {
        ClientConfiguration config = new ClientConfiguration();
        addProxyConfiguration(properties, config);

        @SuppressWarnings("deprecation")
        AWSSecurityTokenService securityTokenService = new AWSSecurityTokenServiceClient(primaryCredentialsProvider,
                                                                                         config);
        String assumeRoleArn = properties.get(ASSUME_ROLE_ARN);
        String assumeRoleName = properties.get(ASSUME_ROLE_NAME);

        STSAssumeRoleSessionCredentialsProvider.Builder builder =
                new STSAssumeRoleSessionCredentialsProvider.Builder(assumeRoleArn, assumeRoleName);
        builder.withStsClient(securityTokenService);

        setMaxSessionTime(builder, properties, MAX_SESSION_TIME);

        String assumeRoleExternalId = properties.get(ASSUME_ROLE_EXTERNAL_ID);
        if (assumeRoleExternalId != null && !assumeRoleExternalId.isEmpty()) {
            builder = builder.withExternalId(assumeRoleExternalId);
        }

        final AWSCredentialsProvider credsProvider = builder.build();

        return credsProvider;
    }

    /**
     * Set the maximum session length on the supplied {@code STSAssumeRoleSessionCredentialsProvider.Builder}.
     * <p>If the maximum session length property is not set, use the default value from the {@link PropertyDescriptor}.
     * If the default value is not set, do not set a value on the builder. In this case, STS defaults to a session
     * length of 900s.</p>
     *
     * @param builder
     *          The builder to set the maximum session length on.
     * @param properties
     *          The provider property map.
     * @param maxSessionTime
     *          The {@link PropertyDescriptor} for the maximum session length property.
     * @throws ProcessException
     *          If either the property value or the default property value is not a valid integer.
     */
    private void setMaxSessionTime(final STSAssumeRoleSessionCredentialsProvider.Builder builder,
                                   final Map<PropertyDescriptor, String> properties,
                                   final PropertyDescriptor maxSessionTime) throws ProcessException {

        String value = StringUtils.trimToNull(properties.get(maxSessionTime));

        if (value == null) {
            final String defaultValue = StringUtils.trimToNull(maxSessionTime.getDefaultValue());

            if (defaultValue == null) {
                return;
            }
            value = defaultValue;
        }

        try {
            final int maxSessionLength = Integer.parseInt(value, 10);
            builder.withRoleSessionDurationSeconds(maxSessionLength);
            return;
        } catch (final NumberFormatException nfe) {
            throw new ProcessException("The value \"" + value + "\" is not a valid value for the " +
                                       maxSessionTime.getDisplayName() +
                                       " property. The error is " + nfe.getMessage() + ".", nfe);
        }
    }

    /**
     * Validate the default session length.
     * @param validationFailureResults
     *          The collection of {@link ValidationResult}s to add to if the default session length not an integer, or
     *          is out of range.
     * @param validationContext
     *          The validation context to use.
     * @param sessionLengthProperty
     *          The {@link PropertyDescriptor} for the session length property.
     */
    private void validateDefaultSessionLength(final Collection<ValidationResult> validationFailureResults,
                                              final ValidationContext validationContext,
                                              final PropertyDescriptor sessionLengthProperty) {

        String sessionLengthValue = sessionLengthProperty.getDefaultValue();

        if (sessionLengthValue == null) {
            return;
        }

        sessionLengthValue = sessionLengthValue.trim();

        final String subject = sessionLengthProperty.getDisplayName() + " (Default Value)";

        if (!validateIntegerProperty(sessionLengthValue, subject, validationFailureResults)) {
            return;
        }

        final int sessionLength = Integer.parseInt(sessionLengthValue);

        // Session time only b/w 900 to 3600 sec (see sts session class)
        if (sessionLength < MINIMUM_SESSION_LENGTH || sessionLength > MAXIMUM_SESSION_LENGTH) {
            final String explanation = " must be between " + MINIMUM_SESSION_LENGTH + " and " +
                                       MAXIMUM_SESSION_LENGTH + " seconds";
            validationFailureResults.add(
                    new ValidationResult.Builder().valid(false)
                                                  .subject(subject)
                                                  .input(sessionLengthProperty.getDefaultValue())
                                                  .explanation(explanation)
                                                  .build());
        }
    }

    /**
     * Validate the session length.
     * @param validationFailureResults
     *          The collection of {@link ValidationResult}s to add to if the session length not an integer, or is out
     *          of range.
     * @param validationContext
     *          The validation context to use.
     * @param sessionLengthProperty
     *          The {@link PropertyDescriptor} for the session length property.
     */
    private void validateSessionLength(final Collection<ValidationResult> validationFailureResults,
                                       final ValidationContext validationContext,
                                       final PropertyDescriptor sessionLengthProperty) {

        if (!super.validateIntegerProperty(validationContext, sessionLengthProperty, validationFailureResults)) {
            return;
        }

        final PropertyValue sessionLengthValue = validationContext.getProperty(sessionLengthProperty);
        final Integer sessionLengthNumber = sessionLengthValue.asInteger();

        if (sessionLengthNumber == null) {
            validateDefaultSessionLength(validationFailureResults, validationContext, sessionLengthProperty);
            return;
        }

        int sessionLength = sessionLengthNumber.intValue();

        // Session time only b/w 900 to 3600 sec (see sts session class)
        if (sessionLength < MINIMUM_SESSION_LENGTH || sessionLength > MAXIMUM_SESSION_LENGTH) {
            final String explanation = " must be between " + MINIMUM_SESSION_LENGTH + " and " +
                                       MAXIMUM_SESSION_LENGTH + " seconds";
            validationFailureResults.add(
                    new ValidationResult.Builder().valid(false)
                                                   .subject(sessionLengthProperty.getDisplayName())
                                                  .input(sessionLengthValue.getValue())
                                                  .explanation(explanation)
                                                  .build());
        }
    }
}
