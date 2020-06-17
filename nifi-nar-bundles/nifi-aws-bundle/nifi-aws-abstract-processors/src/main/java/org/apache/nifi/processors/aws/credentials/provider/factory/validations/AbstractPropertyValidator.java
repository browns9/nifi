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

import java.util.Collection;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;

import com.amazonaws.arn.Arn;

/**
 * Abstract class to implement common validation.
 *
 * <p>Subclasses should only perform validation specific to them.</p>
 *
 * @author Steve Brown, Estafet Ltd.
 *
 */
abstract class AbstractPropertyValidator implements Validator {

    /**
     * The evaluated property value.
     *
     * <p>Gets set when a NiFi expression is evaluated. IF a property does nit have a NiFi expression in the
     * value, this field will be a copy of the raw property value.</p>
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
     * Validate the given {@link PropertyDescriptor} in the given {@link ValidationContext}.
     *
     * <p>This method is called indirectly by the
     * {@code org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory.validate(ValidationContext)}
     * method, which calls the {@code validate(ValidationContext)} method on each
     * class in the {@code org.apache.nifi.processors.aws.credentials.provider.factory.strategies} package.</p>
     *
     * <p>This means that the property might not be set - in which case, the value must not be validated.</p>
     *
     * @param propertyDescriptor
     *          The {@link PropertyDescriptor} for the property being validated.
     * @param validationContext
     *          The {@link ValidationContext} to validate against.
     * @param validationFailureResults
     *          The {@link Collection} of failed validations.
     */
    public void validate(final PropertyDescriptor propertyDescriptor,
                         final ValidationContext validationContext,
                         final Collection<ValidationResult> validationFailureResults) {

        final PropertyValue propertyValue = validationContext.getProperty(propertyDescriptor);

        final String subject = propertyDescriptor.getDisplayName();

        ValidationResult validationResult = validatePropertyPresent(subject, propertyValue);
        if (validationResult == null) {
            validationResult = validatePropertyValue(subject, propertyValue, validationContext);
        }

        if (validationResult == null) {
            return;
        }

        if (!validationResult.isValid()) {
            validationFailureResults.add(validationResult);
        }
    }

    /**
     * Validate a property value.
     *
     * <p>This method is called directly from the {@link PropertyDescriptor#validate(String, ValidationContext)}
     * method.</p>
     *
     * <p>The AWS Java SDK {@link Arn#fromString(String)} method does not validate the contents of fields in the
     * ARN; it validates only that the fields are present.</p>
     *
     * @param subject
     *          What is being validated.
     * @param input
     *          The string (value) to be validated.
     * @param context
     *          The {@link ValidationContext} to use when validating properties.
     * @return
     *          A {@link ValidationResult} object containing the outcome of the validation.
     */
    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {

        ValidationResult validationResult = validateNotEmptyOrBlank(subject, input);

        if (validationResult.isValid()) {
            validationResult = validateExpressionLanguageInPropertyValue(subject, input, context);

            if (validationResult.isValid()) {

                if (isMandatoryProperty() || !StringUtils.isEmpty(evaluatedInput)) {
                    validationResult = validateValue(subject, input);
                }
            }
        }

        return validationResult;
    }

    /**
     * Evaluate a NiFi expression.
     *
     * <p><strong>Note</strong>:<p>
     * <p>Non-existent variables evaluate to an empty string, and do not fail validation.</p>
     * @param subject
     *          What is being validated.
     * @param input
     *          The value being validated.
     * @param context
     *          The context in which to evaluate the NiFi expression.
     * @return
     *          The {@link ValidationResult}. {@code ValidationResult#isValid()} will return {@code false} if the
     *          evaluation failed.
     *
     */
    private ValidationResult evaluateNiFiExpression(final String subject,
                                                    final String input,
                                                    final ValidationContext context) {
        evaluatedInput = input;

        boolean isValid = true;
        String explanation = null;
        if  (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
             try {
                 final PropertyValue propertyValue = context.newPropertyValue(input);
                 evaluatedInput = propertyValue.evaluateAttributeExpressions().getValue();
                 if (isValid && isMandatoryProperty()) {
                     isValid = !StringUtils.isEmpty(evaluatedInput);
                     if (!isValid) {
                         explanation = "The NiFi Expression \"" + input + "\" must not evaluate to an empty string.";
                     }
                }
             } catch (final ProcessException pe) {
                 isValid = false;
                 explanation = "\"" + input  + "\" is not a valid NiFi expression. The error is " +
                               pe.getMessage() + ".";
             }
         }

        return new ValidationResult.Builder().subject(subject)
                                             .input(isValid ? null : input)
                                             .explanation(explanation)
                                             .valid(isValid)
                                             .build();
    }

    /**
     * Validate that the expression is syntactically correct.
     * @param subject
     *          What is being validated.
     * @param input
     *          The NiFi expression being evaluated.
     * @param context
     *          The validation context.
     * @return
     *          The {@link ValidationResult}. {@code ValidationResult#isValid()} will return {@code false} if the
     *          evaluation failed.
     */
    private ValidationResult validateIsValidNiFiExpression(final String subject,
                                                           final String input,
                                                           final ValidationContext context) {

        boolean isValid = true;
        String explanation = null;

        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            try {
                final String reason = context.newExpressionLanguageCompiler().validateExpression(input, true);
                if (!StringUtils.isEmpty(reason)) {
                    isValid = false;
                    explanation = "The value is not a valid NiFi expression. The reason is: " + reason;
                }
            } catch (final Exception e) {
                isValid = false;
                explanation = "an error occurred validating the NiFi expression. The error message is: " +
                              e.getMessage() + ".";
            }
        }

        return new ValidationResult.Builder().subject(subject)
                                             .input(isValid ? null : input)
                                             .valid(isValid)
                                             .explanation(explanation)
                                             .build();
    }

    /**
     * @return {@code true} if the subclass is a mandatory property.
     */
    protected abstract boolean isMandatoryProperty();

    /**
     * Validate the use of the NiFi expression language in the property value.
     *
     * <p>If the property supports the NiFi expression language and the property value contains NiFi expression language
     * terms, this method sets {@link #evaluatedInput} to the evaluated property value. Otherwise,
     * {@link #evaluatedInput} is set to the property's raw value.</p>
     *
     * @param subject
     *          What is being validated (usually the name from the property descriptor).
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
        ValidationResult validationResult = validateIsValidNiFiExpression(subject, input, context);

        if (validationResult.isValid()) {
            validationResult = evaluateNiFiExpression(subject, input, context);
        }

        return validationResult;
    }

    /**
     * Validate that the property valid is not {@code null}, empty or blank.
     * @param subject
     *          The subject - what is being validated.
     * @param input
     *          The value being validated.
     * @return
     *          The {@link ValidationResult}. {@code Validation.isValid()} will return {@code} true if the property
     *          value is OK.
     *
     */
    protected ValidationResult validateNotEmptyOrBlank(final String subject, final String input) {

        boolean isValid = true;
        String explanation = null;
        if (isMandatoryProperty() && StringUtils.isBlank(input)) {
            isValid = false;
            explanation = "The \"" + subject + "\" property cannot be empty or blank.";
        }
        final ValidationResult validationResult = new ValidationResult.Builder()
                                            .subject(subject)
                                            .input(isValid ? null : input)
                                            .valid(isValid)
                                            .explanation(explanation)
                                            .build();
        return validationResult;
    }

    /**
     * Validate that a property exists.
     * @param subject
     *          The subject of the property - i.e. what the property is for.
     * @param propertyValue
     *          The property value.
     * @return
     *          The validation result. {@code null} if the property exists.
     *          Otherwise, {@code ValidationResult#isValid()} will return {@code false}.
     */
    protected ValidationResult validatePropertyPresent(final String subject,
                                                       final PropertyValue propertyValue) {
        boolean isValid = propertyValue != null;
        String explanation = null;
        if (!isValid) {
            explanation = "The \"" + subject + "\" property must exist.";
        }
        final ValidationResult validationResult = new ValidationResult.Builder().subject(subject)
                                                                                .input(null)
                                                                                .valid(isValid)
                                                                                .explanation(explanation)
                                                                                .build();

        return validationResult;
    }

    /**
     * Validate the property value.
     * @param subject
     *          The subject of the property - i.e. what the property is for.
     * @param propertyValue
     *          The {@link PropertyValue} for the property.
     * @param validationContext
     *          The {@link ValidationContext} to use.
     * @return
     *          The {@link ValidationResult}. Will be {@code null} if the property is not set, i.e.
     *          {@code propertyValue.isSet()} returns {@code false}.
     *          Otherwise, {@link ValidationResult#isValid()} will return {@code false} because validation has
     *          failed.
     */
    protected ValidationResult validatePropertyValue(final String subject,
                                                     final PropertyValue propertyValue,
                                                     final ValidationContext validationContext) {

        ValidationResult validationResult = null;
        if (propertyValue.isSet()) {
            final String input = propertyValue.getValue();

            validationResult = validate(subject, input, validationContext);
        }

        return validationResult;
    }

    /**
     * Validate the value in the subclass.
     *
     * <p>Execute whatever specific validation that subclasses require.</p>
     * @param subject
     *          What is being validated - i.e. what is the property for.
     * @param input
     *          The value of the property being validated.
     * @return
     *          A {@link ValidationResult} object containing the outcome of the validation.
     *          {@link ValidationResult#isValid()} will return {@code true} if the the ARN is OK, or {@code false} if
     *          validation fails.
     */
    protected abstract ValidationResult validateValue(final String subject, final String input);
}
