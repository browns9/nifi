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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ExplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AccessKeyPairCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.FileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.NamedProfileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AnonymousCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ImplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AssumeRoleCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.WebIdentityTokenCredentialsStrategy;

import com.amazonaws.auth.AWSCredentialsProvider;


/**
 * Generates AWS credentials in the form of AWSCredentialsProvider implementations for processors
 * and controller services.  The factory supports a number of strategies for specifying and validating
 * AWS credentials, interpreted as an ordered list of most-preferred to least-preferred.  It also supports
 * derived credential strategies like Assume Role, which require a primary credential as an input.
 *
 * Additional strategies should implement CredentialsStrategy, then be added to the strategies list in the
 * constructor.
 *
 * @see org.apache.nifi.processors.aws.credentials.provider.factory.strategies
 */
public class CredentialsProviderFactory {

    /**
     * The list of strategies
     */
    private final List<CredentialsStrategy> strategies = new ArrayList<CredentialsStrategy>();

    /**
     * The constructor.
     *
     * <p>Populates the list of strategies to use.</p>
     */
    public CredentialsProviderFactory() {
        // Primary Credential Strategies
        strategies.add(new ExplicitDefaultCredentialsStrategy());
        strategies.add(new AccessKeyPairCredentialsStrategy());
        strategies.add(new FileCredentialsStrategy());
        strategies.add(new NamedProfileCredentialsStrategy());
        strategies.add(new AnonymousCredentialsStrategy());

        // Implicit Default is the catch-all primary strategy
        strategies.add(new ImplicitDefaultCredentialsStrategy());

        // Derived Credential Strategies
        strategies.add(new AssumeRoleCredentialsStrategy());
        strategies.add(new WebIdentityTokenCredentialsStrategy());
    }

    /**
     * Select the primary strategy, using the supplied properties.
     *
     * @param properties
     *          The properties to match against a strategy in the list of supported strategies.
     * @return
     *          The first strategy that can create a primary credential, using the supplied properties. Otherwise,
     *          return {@code null} because none of the supported strategies can create a primary credential, using the
     *          supplied properties.
     */
    public CredentialsStrategy selectPrimaryStrategy(final Map<PropertyDescriptor, String> properties) {
        for (CredentialsStrategy strategy : strategies) {
            if (strategy.canCreatePrimaryCredential(properties)) {
                return strategy;
            }
        }
        return null;
    }

    /**
     * Select the primary strategy, using the supplied {@link ValidationContext}.
     *
     * @param validationContext
     *          The validation context to use. This contains the properties to match against a strategy in the list of
     *          supported strategies.
     * @return
     *          The first strategy that can create a primary credential, using the properties in the supplied
     *          validation context. Otherwise, return {@code null} because none of the supported strategies can create
     *          a primary credential, using the supplied validation context.
     */
    public CredentialsStrategy selectPrimaryStrategy(final ValidationContext validationContext) {
        final Map<PropertyDescriptor, String> properties = validationContext.getProperties();
        return selectPrimaryStrategy(properties);
    }

    /**
     * Validates AWS credential properties against the configured strategies to report any validation errors.
     * @param validationContext
     *          The validation context to use.
     * @return
     *          A collection of validation errors. Each element in the collection is a {@link ValidationResult} where
     *          the {@link ValidationResult#isValid()} method returns {@code false}.
     */
    public Collection<ValidationResult> validate(final ValidationContext validationContext) {
        final CredentialsStrategy selectedStrategy = selectPrimaryStrategy(validationContext);
        final ArrayList<ValidationResult> validationFailureResults = new ArrayList<ValidationResult>();

        for (CredentialsStrategy strategy : strategies) {
            final Collection<ValidationResult> strategyValidationFailures = strategy.validate(validationContext,
                    selectedStrategy);
            if (strategyValidationFailures != null) {
                validationFailureResults.addAll(strategyValidationFailures);
            }
        }

        return validationFailureResults;
    }

    /**
     * Produces the {@link AWSCredentialsProvider} according to the given property set and the strategies configured in
     * the factory.
     * @param properties
     *           The properties to use to select the primary strategy.
     * @return
     *           The {@code AWSCredentialsProvider} implementation.
     */
    public AWSCredentialsProvider getCredentialsProvider(final Map<PropertyDescriptor, String> properties) {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(properties);
        AWSCredentialsProvider primaryCredentialsProvider = primaryStrategy.getCredentialsProvider(properties);
        AWSCredentialsProvider derivedCredentialsProvider = null;

        for (CredentialsStrategy strategy : strategies) {
            if (strategy.canCreateDerivedCredential(properties)) {
                derivedCredentialsProvider = strategy.getDerivedCredentialsProvider(properties,
                        primaryCredentialsProvider);
                break;
            }
        }

        if (derivedCredentialsProvider != null) {
            return derivedCredentialsProvider;
        } else {
            return primaryCredentialsProvider;
        }
    }
}
