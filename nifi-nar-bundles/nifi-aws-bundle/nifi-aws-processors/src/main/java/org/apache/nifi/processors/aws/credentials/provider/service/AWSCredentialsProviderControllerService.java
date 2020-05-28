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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory;
import org.apache.nifi.reporting.InitializationException;

import com.amazonaws.auth.AWSCredentialsProvider;

import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ACCESS_KEY;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.CREDENTIALS_FILE;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.PROFILE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.SECRET_KEY;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.WEB_IDENTITY_ROLE_SESSION_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.WEB_IDENTITY_TOKEN_FILE;


/**
 * Implementation of the {@link AWSCredentialsProviderService} interface.
 *
 * @see AWSCredentialsProviderService
 */
@CapabilityDescription("Defines credentials for Amazon Web Services processors. " +
        "Uses default credentials without configuration. " +
        "Default credentials support EC2 instance profile/role, default user profile, environment variables, etc. " +
        "Additional options include access key / secret key pairs, credentials file, named profile, and assume role credentials.")
@Tags({ "aws", "credentials","provider" })
public class AWSCredentialsProviderControllerService extends AbstractControllerService implements AWSCredentialsProviderService {

    /**
     * The ARN of the role that will be assumed.
     */
    public static final PropertyDescriptor ASSUME_ROLE_ARN = CredentialPropertyDescriptors.ASSUME_ROLE_ARN;

    /**
     * The name of the role that will be assumed.
     */
    public static final PropertyDescriptor ASSUME_ROLE_NAME = CredentialPropertyDescriptors.ASSUME_ROLE_NAME;

    /**
     * The maximum length of the session in seconds.
     */
    public static final PropertyDescriptor MAX_SESSION_TIME = CredentialPropertyDescriptors.MAX_SESSION_TIME;

    /**
     * The list of properties supported by this service.
     */
    private static final List<PropertyDescriptor> properties;

    /**
     * Define the properties supported by this service.
     */
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(USE_DEFAULT_CREDENTIALS);
        props.add(ACCESS_KEY);
        props.add(SECRET_KEY);
        props.add(CREDENTIALS_FILE);
        props.add(PROFILE_NAME);
        props.add(USE_ANONYMOUS_CREDENTIALS);
        props.add(ASSUME_ROLE_ARN);
        props.add(ASSUME_ROLE_NAME);
        props.add(MAX_SESSION_TIME);
        props.add(ASSUME_ROLE_EXTERNAL_ID);
        props.add(ASSUME_ROLE_PROXY_HOST);
        props.add(ASSUME_ROLE_PROXY_PORT);
        props.add(WEB_IDENTITY_ROLE_ARN);
        props.add(WEB_IDENTITY_ROLE_SESSION_NAME);
        props.add(WEB_IDENTITY_TOKEN_FILE);
        properties = Collections.unmodifiableList(props);
    }

    /**
     * The credentials provider used by this service.
     * @see com.amazonaws.auth.AWSCredentialsProvider
     */
    private volatile AWSCredentialsProvider credentialsProvider;


    /**
     * The credentials provider factory used to create the credentials provider.
     * @see CredentialsProviderFactory
     */
    protected final CredentialsProviderFactory credentialsProviderFactory = new CredentialsProviderFactory();


    /**
     * Get a list of the property descriptor objects that are supported by this processor.
     *
     * @return PropertyDescriptor objects this processor currently supports.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Get the credentials provider.
     * @return The credentials provider.
     * @throws ProcessException never.
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    @Override
    public AWSCredentialsProvider getCredentialsProvider() throws ProcessException {
        return credentialsProvider;
    }

    /**
     * Perform validation on the properties.
     *
     * <p>Since each property is validated as it is set, this allows validation of groups of properties together.</p>
     *
     * <p>This method will be called only when it has been determined that all property values are valid according to
     * their corresponding validators returned by {@link PropertyDescriptor#getValidators()}.</p>
     *
     * <p>There are these implementations of {@link ValidationContext}:</p>
     * <ul>
     *   <li>{@code org.apache.nifi.bootstrap.notification.NotificationValidationContext}</li>
     *   <li>{@code org.apache.nifi.processor.StandardValidationContext}</li>
     *   <li>{@code org.apache.nifi.stateless.core.StatelessValidationContext}</li>
     *   <li>{@code org.apache.nifi.script.impl.FilteredPropertiesValidationContextAdapter}</li>
     * </ul>
     *
     * @param validationContext
     *          Provides a mechanism for obtaining externally managed values, such as property values, and supplies
     *          convenience methods for operating on those values.
     *
     * @return
     *          A {@link Collection} of {@link ValidationResult} objects that will be added to any
     *          other validation findings.
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Collection<ValidationResult> validationFailureResults =
                credentialsProviderFactory.validate(validationContext);
        return validationFailureResults;
    }

    /**
     * This method is called when this service is enabled or restarted.
     *
     * <p>This method will be called every time a user enables the service. Additionally, each time that NiFi is
     * restarted, this method will be called if NiFi is configured to "auto-resume state" and this service is
     * enabled.</p>
     *
     * <p>There are two implementations of the {@code ConfigurationContext} interface:
     * {@code org.apache.nifi.controller.service.StandardConfigurationContext} and
     * {@code org.apache.nifi.controller.service.StatelessConfigurationContext}.
     *
     * @param context
     *          The configuration for this service, in a {@link ConfigurationContext} object.
     * @throws InitializationException
     *          Never.
     */
    @SuppressWarnings("unused")
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final Map<PropertyDescriptor, String> properties = context.getProperties();
        properties.keySet().forEach(propertyDescriptor -> {
            if (propertyDescriptor.isExpressionLanguageSupported()) {
                properties.put(propertyDescriptor,
                        context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue());
            }
        });
        credentialsProvider = credentialsProviderFactory.getCredentialsProvider(properties);
        getLogger().debug("Using credentials provider: " + credentialsProvider.getClass());
    }

    /**
     * Create a human-readable representation of this object.
     *
     * @return
     *          The human-readable string.
     */
    @Override
    public String toString() {
        return "AWSCredentialsProviderService[id=" + getIdentifier() + "]";
    }
}