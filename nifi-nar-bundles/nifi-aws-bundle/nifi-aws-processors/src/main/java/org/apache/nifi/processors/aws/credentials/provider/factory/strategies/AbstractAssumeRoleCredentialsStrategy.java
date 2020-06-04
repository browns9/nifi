/**
 * Copyright (c) Elsevier PLC 2020. All Rights reserved.
 */
package org.apache.nifi.processors.aws.credentials.provider.factory.strategies;

import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.exception.ProcessException;

import com.amazonaws.ClientConfiguration;

/**
 * Common methods required for credential provider strategies that assume a role.
 *
 * <p>Currently, these are:</p>
 * <ul>
 *      <li>{@link org.apache.nifi.processors.aws.credentials.provider.factory.strategies.WebIdentityTokenCredentialsStrategy}</li>
 *      <li>{@link org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AssumeRoleCredentialsStrategy}</li>
 * </ul>
 * @author Steve Brown, Estafet Ltd.
 *
 */
public abstract class AbstractAssumeRoleCredentialsStrategy extends AbstractCredentialsStrategy {

    /**
     * The minimum legal value for a port number.
     */
    private static final int MINIMUM_PORT_NUMBER = 0;

    /**
     * The maximum legal value for a port number.
     */
    private static final int MAXIMUM_PORT_NUMBER = 65_535;

    /**
     * Construct from the strategy name and the required properties.
     * @param strategyName
     *          The strategy name.
     * @param strategyProperties
     *          The properties required to create a particular {@link com.amazonaws.auth.AWSCredentialsProvider}.
     */
    public AbstractAssumeRoleCredentialsStrategy(final String strategyName,
                                                 final PropertyDescriptor[] strategyProperties) {
        super(strategyName, strategyProperties);
    }

    /**
     * Check whether or not the proxy variables are present.
     * @param properties
     *          The map from {@link PropertyDescriptor} to the property value.
     * @return
     *          {@code true} if both the proxy host name and proxy port properties are present.
     */
    public boolean proxyVariablesValidForAssumeRole(final Map<PropertyDescriptor, String> properties){
        final String assumeRoleProxyHost = properties.get(ASSUME_ROLE_PROXY_HOST);
        final String assumeRoleProxyPort = properties.get(ASSUME_ROLE_PROXY_PORT);
        if (StringUtils.isEmpty(assumeRoleProxyHost) || StringUtils.isEmpty(assumeRoleProxyPort)) {
            return false;
        }
        return true;
    }

    /**
     * Validate that the proxy configuration is valid.
     *
     * <p>Either both the proxy host and proxy port must be defined, or neither must be defined.</p>
     *
     * @param validationContext
     *          The {@link ValidationContext} to use.
     * @param validationFailureResults
     *          The {@link Collection} of {@link ValidationResult}s. If there are no errors, nothing is added to the
     *          failure results.
     */
    protected void validateProxyConfiguration(final ValidationContext validationContext,
                                             final Collection<ValidationResult> validationFailureResults) {
        final boolean assumeRoleProxyHostIsSet = validationContext.getProperty(ASSUME_ROLE_PROXY_HOST).isSet();
        final PropertyValue proxyPortValue = validationContext.getProperty(ASSUME_ROLE_PROXY_PORT);
        final boolean assumeRoleProxyPortIsSet = proxyPortValue.isSet();

        // Both proxy host and proxy port are required if either are present.
        if (assumeRoleProxyHostIsSet ^ assumeRoleProxyPortIsSet) {
            validationFailureResults.add(
                    new ValidationResult.Builder().input("Assume Role Proxy Host and Port")
                                                  .valid(false)
                                                  .explanation("Assume role with proxy requires both host and port " +
                                                               "for the proxy to be set")
                                                  .build());
        }

        if (assumeRoleProxyPortIsSet) {
            validateProxyPort(validationContext, ASSUME_ROLE_PROXY_PORT, validationFailureResults);
        }
    }

    /**
     * Validate the proxy port value.
     * @param validationContext
     *          The {@link ValidationContext} to use.
     * @param property
     *          The {@link PropertyDescriptor} for the proxy port.
     * @param validationFailureResults
     *          The {@link Collection} of {@link ValidationResult}s. If there are no errors, nothing is added to the
     *          failure results.
     */
    private void validateProxyPort(final ValidationContext validationContext,
                                   final PropertyDescriptor property,
                                   final Collection<ValidationResult> validationFailureResults) {

        if (!validateIntegerProperty(validationContext, property, validationFailureResults)) {
            return;
        }

        final Integer proxyPort = validationContext.getProperty(property).asInteger();

        if (proxyPort == null) {
            return;
        }

        final int value = proxyPort.intValue();
        if (value < MINIMUM_PORT_NUMBER || value > MAXIMUM_PORT_NUMBER) {
            final String propertyValue = validationContext.getProperty(property).getValue();
            final String explanation =
                "the proxy port value must be between " + MINIMUM_PORT_NUMBER + " and " + MAXIMUM_PORT_NUMBER + ".";

            validationFailureResults.add(
                new ValidationResult.Builder().subject(property.getDisplayName())
                                              .input(propertyValue)
                                              .valid(false)
                                              .explanation(explanation)
                                              .build());
        }
    }

    /**
     * Validate an integer value.
     *
     * <p>Allows the property to be not set.</p>
     *
     * @param validationContext
     *          The {@link ValidationContext} to use.
     * @param property
     *          The {@link PropertyDescriptor} for the integer property.
     * @param validationFailureResults
     *          The {@link Collection} of {@link ValidationResult}s. If there are no errors, nothing is added to the
     *          failure results.
     * @return
     *          {@code true} if the validation succeeds or the property is not set in the {@link ValidationContext}.
     */
    protected boolean validateIntegerProperty(final ValidationContext validationContext,
                                              final PropertyDescriptor property,
                                              final Collection<ValidationResult> validationFailureResults) {
        final PropertyValue propertyValue = validationContext.getProperty(property);

        if (propertyValue == null) {
            return true;
        }

        try {
            propertyValue.asInteger();
            return true;
        } catch (final NumberFormatException nfe) {
            final String explanation = "is not a valid integer. The error is \"" + nfe.getMessage() + "\".";
            validationFailureResults.add(
                    new ValidationResult.Builder().subject(property.getDisplayName())
                                                  .input(propertyValue.getValue())
                                                  .valid(false)
                                                  .explanation(explanation)
                                                  .build());
            return false;
        }
    }

    /**
     * Validate an integer value.
     * @param propertyValue
     *          The value of the property.
     * @param subject
     *          The display name from the {@link PropertyDescriptor} for the integer property.
     * @param validationFailureResults
     *          The {@link Collection} of {@link ValidationResult}s. If there are no errors, nothing is added to the
     *          failure results.
     * @return
     *          {@code true} if the validation succeeds.
     */
    protected boolean validateIntegerProperty(final String propertyValue,
                                              final String subject,
                                              final Collection<ValidationResult> validationFailureResults) {

        if (propertyValue == null) {
            return true;
        }

        try {
            Integer.parseInt(propertyValue, 10);
            return true;
        } catch (final NumberFormatException nfe) {
            final String explanation = "is not a valid integer. The error is \"" + nfe.getMessage() + "\".";
            validationFailureResults.add(
                    new ValidationResult.Builder().subject(subject)
                                                  .input(propertyValue)
                                                  .valid(false)
                                                  .explanation(explanation)
                                                  .build());
            return false;
        }
    }

    /**
     * Add the proxy configuration to the AWS {@link ClientConfiguration}.
     *
     * @param properties
     *          The map from {@link PropertyDescriptor}s to property values.
     * @param awsClientConfiguration
     *          The AWS client configuration to amend.
     * @throws ProcessException
     *          If the proxy port is not an integer.
     */
    protected void addProxyConfiguration(final Map<PropertyDescriptor, String> properties,
                                         final ClientConfiguration awsClientConfiguration)
                                                         throws ProcessException {
        // If proxy variables are set, then create Client Configuration with those values
        if (proxyVariablesValidForAssumeRole(properties)) {
            final String assumeRoleProxyHost = properties.get(ASSUME_ROLE_PROXY_HOST);
            final int assumeRoleProxyPort = getProxyPort(properties);
            awsClientConfiguration.withProxyHost(assumeRoleProxyHost);
            awsClientConfiguration.withProxyPort(assumeRoleProxyPort);
        }
    }

    /**
     * Get the proxy port number.
     *
     * <p>Wraps any {@link NumberFormatException} in a {@link ProcessException}.</p>
     * @param properties
     *          The properties to look up.
     * @return
     *          The proxy port number.
     * @throws ProcessException
     *          If the port string is not a valid integer string. This shouldn't happen because the proxy port property
     *          should have been validated before this method is called.
     */
    private int getProxyPort(final Map<PropertyDescriptor, String> properties) throws ProcessException {
        final String value = properties.get(ASSUME_ROLE_PROXY_PORT);

        try {
            final int port = Integer.parseInt(value);
            return port;
        } catch (final NumberFormatException nfe) {
            throw  new ProcessException("The assume role proxy port value \"" + value +
                                        "\" is not an integer value.", nfe);
        }
    }
}
