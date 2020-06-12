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
package org.apache.nifi.processors.aws.credentials.provider.factory.validations;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * @author Steve Brown, Estafet Ltd.
 *
 */
abstract class AbstractPropertyValidator implements Validator {

    /**
     * The evaluated property value.
     */
    protected String evaluatedInput;


    /**
     * Constructor.
     *
     */
    protected AbstractPropertyValidator() {
        super();
    }

    /**
     * Validate the use of the NiFi expression language in the property value.
     *
     * <p>If the property supports the NiFi expression language and the property value contains NiFi expression language
     * terms, this method sets {@link #evaluatedInput} to the evaluated property value. Otherwise,
     * {@link #evaluatedInput} is set to the property's raw value.</p>
     *
     * @param subject
     *          What is being validated (usually the display name from the property descriptor).
     * @param input
     *          The string (value) to be validated.
     * @param context
     *          The {@link ValidationContext} to use when validating properties.
     * @return
     *          The validation result in a {@link ValidationResult} object. {@link ValidationResult#isValid()} will
     *          return {@code false} if validation fails.
     */
    protected ValidationResult validateExpressionLanguageInPropertyValue(final String subject,
                                                                         final String input,
                                                                         final ValidationContext context) {

        // Check the NiFi expression.
        final ValidationResult evaluationResult =
                StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR.validate(subject, input, context);

         if (!evaluationResult.isValid()) {
             return evaluationResult;
         }

         evaluatedInput = input;
         if  (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
             try {
                 final PropertyValue propertyValue = context.newPropertyValue(input);
                 evaluatedInput = propertyValue.evaluateAttributeExpressions().getValue();
             } catch (final ProcessException pe) {
                 return new ValidationResult.Builder().subject(subject)
                                                      .input(input)
                                                      .explanation("not a valid NiFi expression. The error is " +
                                                                   pe.getMessage() + ".")
                                                      .valid(false)
                                                      .build();
             }
         }
         return new ValidationResult.Builder().subject(subject)
                                              .input(input)
                                              .explanation(null)
                                              .valid(true)
                                              .build();
    }

}
