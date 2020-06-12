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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;

import com.amazonaws.arn.Arn;

/**
 * Class to validate an AWS ARN.
 *
 * @see <a href="http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#genref-arns">AWS ARN</a>
 *
 * @author Steve Brown, Estafet Ltd.
 *
 */
public final class AwsArnValidator extends AbstractPropertyValidator {

    /**
     * Validate an AWS ARN.
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


        if (StringUtils.isBlank(input)) {

            return new ValidationResult.Builder().subject(subject)
                                                 .input(input)
                                                 .valid(false)
                                                 .explanation("an ARN cannot be empty or blank.")
                                                 .build();
        }

        final ValidationResult evaluationResult = validateExpressionLanguageInPropertyValue(subject, input, context);

        if (!evaluationResult.isValid()) {
            return evaluationResult;
        }

        try {
            // Validate the ARN.
            @SuppressWarnings("unused")
            final Arn arn = Arn.fromString(evaluatedInput);
        } catch (final IllegalArgumentException iae) {

            return new ValidationResult.Builder().subject(subject)
                                                 .input(input)
                                                 .valid(false)
                                                 .explanation(iae.getMessage())
                                                 .build();
        }

        return new ValidationResult.Builder().subject(subject)
                                             .input(input)
                                             .valid(true)
                                             .explanation(null)
                                             .build();
    }

    /**
     * Validate the Web Role ARN.
     *
     * <p>This method is called indirectly by the
     * {@code org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory.validate(ValidationContext)}
     * method, which calls the {@code validate(ValidationContext)} method on each
     * class in the {@code org.apache.nifi.processors.aws.credentials.provider.factory.strategies} package.</p>
     *
     * <p>This means that the value of the {@link PropertyDescriptor} can be {@code null}, in which case, the value
     * must not be validated.</p>
     *
     * @param webIdentityRoleArnProperty
     *          The {@link PropertyDescriptor} for the Web Identity Role ARN to validate.
     * @param validationContext
     *          The {@link ValidationContext} to validate against.
     * @param validationFailureResults
     *          The {@link Collection} of failed {@link ValidationResult} validation results.
     */
    public void validate(final PropertyDescriptor webIdentityRoleArnProperty,
                         final ValidationContext validationContext,
                         final Collection<ValidationResult> validationFailureResults) {

        final PropertyValue webIdentityRoleArnValue = validationContext.getProperty(webIdentityRoleArnProperty);

        ValidationResult validationResult = null;
        if (webIdentityRoleArnValue == null) {
            validationResult = new ValidationResult.Builder()
                                             .subject("Web Identity Role ARN")
                                             .input(null)
                                             .valid(false)
                                             .explanation("The Web Identity Role ARN must be specified.")
                                             .build();
        } else {
            final String subject = webIdentityRoleArnProperty.getName();
            final String input = webIdentityRoleArnValue.getValue();

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
     * Validate the Web Identity Role Session Name.
     *
     * <p>The Web Identity Role Session Name must not be {@code null}, empty, or  blank. </p>
     * <p>The Web Identity Role Session Name must match the regular expression in
     * {@link org.apache.nifi.processors.aws.credentials.provider.factory.validations.AwsWebIdentityRoleSessionNameValidator}.</p>
     * <p>The Web Identity Role Session Name can contain NiFi expressions.</p>
     *
     * @param webIdentityRoleSessionProperty
     *          The Web Identity Role Session property to validate.
     * @param validationContext
     *          The {@link ValidationContext} to validate against.
     * @param validationFailureResults
     *          The {@link Collection} of failed validations.
     */
    public void validateRoleSessionName(final PropertyDescriptor webIdentityRoleSessionProperty,
                                        final ValidationContext validationContext,
                                        final Collection<ValidationResult> validationFailureResults) {

        final PropertyValue roleSessionNameValue = validationContext.getProperty(webIdentityRoleSessionProperty);

        ValidationResult validationResult = null;
        if (roleSessionNameValue == null) {
            validationResult = new ValidationResult.Builder()
                    .subject("Web Identity Role Session Name")
                    .input(null)
                    .valid(false)
                    .explanation("The Web Identity Role ARN must be specified.")
                    .build();
        } else {
            final String roleSessionName = roleSessionNameValue.getValue();
            validationResult = webIdentityRoleSessionProperty.validate(roleSessionName, validationContext);
        }

        if (!validationResult.isValid()) {
            validationFailureResults.add(validationResult);
        }
    }
}
