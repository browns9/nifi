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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.exception.ProcessException;


/**
 * Class to validate an AWS Web Identity Token File.
 *
 * @author Steve Brown, Estafet Ltd.
 *
 */
public final class AwsWebIdentityTokenFileValidator implements Validator {

    /**
     * {@code true} if the value being validated can contain a NiFi expression.
     */
    final boolean allowExpressionLanguage;

    /**
     * Constructor.
     *
     * <p>The default is to allow NiFi expressions.</p>
     */
    public AwsWebIdentityTokenFileValidator() {
        this(true);
    }

    /**
     * Constructor.
     * @param allowExpressionLanguageFlag
     *          {@code true} to allow NiFi expressions.
     */
    public AwsWebIdentityTokenFileValidator(final boolean allowExpressionLanguageFlag) {
        super();
        allowExpressionLanguage = allowExpressionLanguageFlag;
    }

    /**
     * Validate an AWS Web Identity Token File.
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

        ValidationResult validationResult = null;
        if (StringUtils.isBlank(input)) {
            validationResult = new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("the Web Identity Token File cannot be empty or blank.")
                    .build();
        } else {
            validationResult = internalValidate(subject, input, context);
        }

        return validationResult;
    }

    /**
     * Validate the Web Identity Token File.
     *
     * <p>This method is called indirectly by the
     * {@code org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory.validate(ValidationContext)}
     * method, which calls the {@code validate(ValidationContext)} method on each
     * class in the {@code org.apache.nifi.processors.aws.credentials.provider.factory.strategies} package.</p>
     *
     * <p>This means that the value of the {@link PropertyDescriptor} can be {@code null}, in which case, the value
     * must not be validated.</p>
     *
     * <p>The Web Identity Role Token File must be an existing file. </p>
     *
     * @param webIdentityTokenFileProperty
     *          The {@link PropertyDescriptor} for the Web Identity Token File property to validate.
     * @param validationContext
     *          The {@link ValidationContext} to validate against.
     * @param validationFailureResults
     *          The {@link Collection} of failed validations.
     */
    public void validate(final PropertyDescriptor webIdentityTokenFileProperty,
                         final ValidationContext validationContext,
                         final Collection<ValidationResult> validationFailureResults) {

        final PropertyValue webIdentityTokenFileValue = validationContext.getProperty(webIdentityTokenFileProperty);

        ValidationResult validationResult = null;
        if (webIdentityTokenFileValue == null) {
            validationResult = new ValidationResult.Builder()
                    .subject("Web Identity Token File")
                    .input(null)
                    .valid(false)
                    .explanation("The Web Identity Token must be specified.")
                    .build();
        } else {
            final String subject = webIdentityTokenFileProperty.getName();
            final String input = webIdentityTokenFileValue.getValue();

            if (input == null) {
                return;
            }
            validationResult = validate(subject, input, validationContext);
        }

        if (!validationResult.isValid()) {
            validationFailureResults.add(validationResult);
        }
    }

    /**
     * Validate an AWS Web Identity Token File.
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
    private ValidationResult internalValidate(final String subject,
                                              final String input,
                                              final ValidationContext context) {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder().subject(subject)
                                                 .input(input).explanation("Expression Language Present")
                                                 .valid(true)
                                                 .build();
        }

        String evaluatedValue = null;
        try {
            evaluatedValue = getEvaluatedValue(input, context);
        } catch (final ProcessException pe) {
            return new ValidationResult.Builder().subject(subject)
                                                 .input(input)
                                                 .valid(false)
                                                 .explanation(" the input contains an inavlid NiFi expression. " +
                                                 "The error is\n" +
                                                 pe.getMessage() + "\n")
                                                 .build();

        }

        Path path = Paths.get(evaluatedValue);

        try {
            path = path.toRealPath();
        } catch (final IOException ioe) {
            final String explanation = "An error occurred getting the real path from " + evaluatedValue + ". " +
                                       "This can happen if the effective user does not have permissions to " +
                                       "access all the components of the path. The error is\n" +
                                       ioe.getMessage() + "\n";
            return new ValidationResult.Builder().subject(subject)
                                                 .input(input)
                                                 .valid(false)
                                                 .explanation(explanation)
                                                 .build();
        }

        final boolean isRegularFile = Files.isRegularFile(path);

        final String explanation = isRegularFile ? null : "The path + " + path.toString() +
                                                          " does not exist, or is not a regular file, " +
                                                          "or it cannot be determined whether the file is a regular " +
                                                          "file or not.";
        return new ValidationResult.Builder().subject(subject)
                                             .input(input)
                                             .valid(isRegularFile)
                                             .explanation(explanation)
                                             .build();
    }

    /**
     * Evaluate the NiFi expressions in the {@code input}.
     * @param input
     *          The string to evaluate.
     * @param context
     *          The {@link ValidationContext} to evaluate the string in.
     * @return
     *          The evaluated input.
     * @throws ProcessException
     *          If the input contains an invalid NiFI expression.
     */
    private String getEvaluatedValue(final String input,
                                     final ValidationContext context) throws ProcessException {
        String evaluated = input;
        if (allowExpressionLanguage) {
            evaluated = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();
        }
        return evaluated;
    }
}
