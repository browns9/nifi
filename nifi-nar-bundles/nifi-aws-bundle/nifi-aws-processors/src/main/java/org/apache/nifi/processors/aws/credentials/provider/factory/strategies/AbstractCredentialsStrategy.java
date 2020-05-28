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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;

import com.amazonaws.auth.AWSCredentialsProvider;


/**
 * Partial implementation of CredentialsStrategy to support most simple property-based strategies.
 */
public abstract class AbstractCredentialsStrategy implements CredentialsStrategy {

    /**
     * The strategy name.
     */
    private final String name;

    /**
     * The properties required to create a particular {@link AWSCredentialsProvider}.
     */
    private final PropertyDescriptor[] requiredProperties;

    /**
     * Construct from the strategy name and the required properties.
     * @param name
     *          The strategy name.
     * @param requiredProperties
     *          The properties required to create a particular {@link AWSCredentialsProvider}.
     */
    public AbstractCredentialsStrategy(String name, PropertyDescriptor[] requiredProperties) {
        this.name = name;
        this.requiredProperties = requiredProperties;
    }

    /**
     * Determines if this strategy can create a primary credential provider, using the given properties.
     *
     * @param properties
     *          The AWS provider properties specified by the user.
     * @return
     *          {@code true} if the primary credential provider can be created.
     */
    @Override
    public boolean canCreatePrimaryCredential(Map<PropertyDescriptor, String> properties) {
        for (PropertyDescriptor requiredProperty : requiredProperties) {
            boolean containsRequiredProperty = properties.containsKey(requiredProperty);
            String propertyValue = properties.get(requiredProperty);
            boolean containsValue = propertyValue != null;
            if (!containsRequiredProperty || !containsValue) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validates the properties belonging to this strategy, given the selected primary strategy.
     *
     * <p>Errors may result from individually malformed properties, invalid combinations of properties, or
     * inappropriate use of properties not consistent with the primary strategy.</p>
     * @param validationContext
     *          The configuration to validate.
     * @param primaryStrategy
     *          The prevailing primary strategy.
     * @return
     *          Validation errors as a {@link Collection} of {@link ValidationResult} objects.
     */
    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        boolean thisIsSelectedStrategy = this == primaryStrategy;
        String requiredMessageFormat = "property %1$s must be set with %2$s";
        String excludedMessageFormat = "property %1$s cannot be used with %2$s";
        String failureFormat = thisIsSelectedStrategy ? requiredMessageFormat : excludedMessageFormat;
        Collection<ValidationResult> validationFailureResults = null;

        for (PropertyDescriptor requiredProperty : requiredProperties) {
            boolean requiredPropertyIsSet = validationContext.getProperty(requiredProperty).isSet();
            if (requiredPropertyIsSet != thisIsSelectedStrategy) {
                String message = String.format(failureFormat, requiredProperty.getDisplayName(),
                        primaryStrategy.getName());
                if (validationFailureResults == null) {
                    validationFailureResults = new ArrayList<ValidationResult>();
                }
                validationFailureResults.add(new ValidationResult.Builder()
                        .subject(requiredProperty.getDisplayName())
                        .valid(false)
                        .explanation(message).build());
            }
        }

        return validationFailureResults;
    }

    /**
     * Creates an {@link AWSCredentialsProvider} instance for this strategy, given the properties defined by the user.
     *
     * @param properties
     *          The AWS provider properties specified by the user.
     * @return
     *          An {@link AWSCredentialsProvider} instance.
     */
    @Override
    public abstract AWSCredentialsProvider getCredentialsProvider(Map<PropertyDescriptor, String> properties);

    /**
     * Get the name of the strategy.
     *
     * <p>The name should be suitable for displaying to a user in validation messages.</p>
     * @return
     *          The strategy name.
     */
    @Override
    public String getName() {
        return name;
    }


    /**
     * Determines if this strategy can create derived credentials using the given properties.
     *
     * @param properties
     *          The AWS provider properties specified by the user.
     * @return
     *          Always returns {@code false}.
     */
    @Override
    public boolean canCreateDerivedCredential(Map<PropertyDescriptor, String> properties) {
        return false;
    }

    /**
     * Creates an {@link AWSCredentialsProvider} instance for this strategy, given the properties defined by the user
     * and the AWSCredentialsProvider from the winning primary strategy.
     *
     * @param properties
     *          The AWS provider properties specified by the user.
     *
     * @param primaryCredentialsProvider
     *          The {@link AWSCredentialsProvider} specified by the winning strategy.
     *
     * @return
     *          The derived {@link AWSCredentialsProvider}.
     * @throws UnsupportedOperationException
     *          Because this is an unsupported operation.
     */
    @Override
    public AWSCredentialsProvider getDerivedCredentialsProvider(Map<PropertyDescriptor, String> properties,
                                                                AWSCredentialsProvider primaryCredentialsProvider) {
        throw new UnsupportedOperationException();
    }


    /**
     * Show the strategy name and required properties for debugging.
     */
    @Override
    public String toString() {
        return getName() + ": Required Properties are " + Arrays.toString(requiredProperties);
    }

}
