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
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.TestRunner;
import org.junit.Assert;

/**
 * The set of properties used to test a service.
 *
 * <p>This class is intended to minimise repeating the same code in every test method.</p>
 * <p>Example usage:</p>
 *
 * <pre>

    private TestRunner runner = null;
    private AWSCredentialsProviderControllerService serviceImpl = null;
    private TestPropertySet testPropertySet = null;

    &#64;Before
    public void setUp() throws Exception {

        runner = TestRunners.newTestRunner(FetchS3Object.class);
        serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService(CREDENTIALS_PROVIDER_ID, serviceImpl);
        testPropertySet = new TestPropertySet(serviceImpl);
    }

    &#64;After
    public void tearDown() {
        runner.shutdown();
    }

    &#64;Test
    public void someTestMethod() {
        testPropertySet.addProperty(PropertyDescriptor1, property1Value)
                       .addProperty(PropertyDescriptor2, property2Value)
        .
        // Add more properties to the test property set.
        .
                       .setAllProperties(runner)
                       .assertAllPropertiesAreValid();
    }
 * </pre>
 *
 * @author Steve Brown, Estafet Ltd.
 *
 */
public class TestPropertySet {

    /**
     * Holds a {@link PropertyDescriptor} and its associated value.
     * @author Steve Brown, Etsafet Ltd.
     *
     */
    private static class PropertyDescriptorAndValue {

        /**
         * The property descriptor.
         */
        final PropertyDescriptor descriptor;

        /**
         * The property value.
         */
        final String value;

        /**
         * The validation result;
         */
        ValidationResult validationResult = null;

        /**
         * Constructor.
         * @param theDescriptor
         *          The {@link PropertyDescriptor} to use.
         * @param theValue
         *          The value associated with the {@code PropertyDescriptor}.
         */
        public PropertyDescriptorAndValue(final PropertyDescriptor theDescriptor, final String theValue) {
            descriptor = theDescriptor;
            value = theValue;
        }
    }

    /**
     * The array of properties.
     */
    private final List<PropertyDescriptorAndValue> properties;

    /**
     * The credentials provider controller service implementation.
     */
    private final AWSCredentialsProviderControllerService service;

    /**
     * Constructor.
     * @param theService
     *          The authentication service
     */
    public TestPropertySet(final AWSCredentialsProviderControllerService theService) {
        service = theService;
        properties = new ArrayList<>();
    }

    /**
     * Add a {@link PropertyDescriptorAndValue} to the list of properties.
     * @param descriptor
     *          The {@link PropertyDescriptor}.
     * @param value
     *          The property value.
     * @return
     *          This object.
     */
    public TestPropertySet addProperty(final PropertyDescriptor descriptor, final String value) {
        properties.add(new PropertyDescriptorAndValue(descriptor, value));
        return this;
    }

    /**
     * Set all the properties on the specified {@link TestRunner}.
     * @param runner
     *          The {@code TestRunner} to use.
     * @return
     *          This object.
     */
    public TestPropertySet setAllProperties(final TestRunner runner) {
        for (final PropertyDescriptorAndValue property : properties) {
            final ValidationResult validationResult = runner.setProperty(service, property.descriptor, property.value);

            property.validationResult = validationResult;
        }
        return this;
    }

    /**
     * Assert that all the properties are valid.
     */
    public void assertAllPropertiesAreValid() {
        Assert.assertTrue(makeAssertMessage(), allPropertiesValid());
    }

    /**
     * @return {@code true} if all the properties are valid.
     */
    private boolean allPropertiesValid() {
        boolean allPropertiesValid = true;
        for (final PropertyDescriptorAndValue property : properties) {

            if (property.validationResult == null) {
                throw new IllegalStateException("The " + property.descriptor + " property has no validation result." +
                                                "The setAllProperties() method must be called before the " +
                                                "allPropertiesValid() method is called.");
            }
            if (!property.validationResult.isValid()) {
                allPropertiesValid = false;
                break;
            }
        }

        return allPropertiesValid;
    }

    /**
     * Make the message used by the JUnit assert.
     * @return
     *          The message.
     */
    private String makeAssertMessage() {
        final StringBuilder builder = new StringBuilder(1024);

        builder.append("At least one credential provider property ")
               .append("did not validate correctly:\n");

        for (final PropertyDescriptorAndValue property: properties) {

            if (property.validationResult == null) {
                throw new IllegalStateException("The " + property.descriptor + " property has no validation result." +
                                                "The setAllProperties() method must be called before the " +
                                                "makeAssertMessage() method is called.");
            }

            builder.append(property.descriptor.toString())
                   .append(" : ").append(property.validationResult.toString()).append("\n");
        }
        return builder.toString();
    }
}
