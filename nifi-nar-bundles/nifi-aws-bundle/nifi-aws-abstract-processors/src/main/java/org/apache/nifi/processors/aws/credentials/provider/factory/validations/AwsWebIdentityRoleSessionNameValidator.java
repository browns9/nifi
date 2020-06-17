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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;

/**
 * Class to validate an AWS Web Identity Role Session Name.
 *
 * <p><strong>Note</strong>:</p>
 * <p>The AWS Role session name is an optional parameter. If it is not provided, then the credential provider strategy
 * class generates one in the {@code getDerivedCredentialsProvider(Map<PropertyDescriptor,String>, AWSCredentialsProvider)}
 * method.</p>
 *
 * <p>If a role session provider property is defined, the value must match the regular expression for an AWS Role Session
 * Name.</p>
 *
 * @author Steve Brown, Estafet Ltd.
 *
 */
public final class AwsWebIdentityRoleSessionNameValidator extends AbstractPropertyValidator {

    /**
     * Regular Expression {@link Pattern} to match a web identity role session name.
     *
     */
    private static final Pattern WEB_IDENTITY_ROLE_SESSION_NAME_PATTERN;

    /**
     * The minimum length of a web identity role session name.
     */
    private static final int MINIMUM_WEB_IDENTITY_ROLE_SESSION_NAME_LENGTH = 8;

    /**
     * The maximum length of a web identity role session name.
     */
    private static final int MAXIMUM_WEB_IDENTITY_ROLE_SESSION_NAME_LENGTH = 64;

    /**
     * Regular Expression to match a web identity role session name.
     *
     * <p>This is an excerpt from the <a href="https://docs.aws.amazon.com/cli/latest/reference/sts/assume-role-with-web-identity.html">
     * AWS CLI documentation for {@code assume-role-with-web-identity}</a></p>.
     * <code>
     * --role-session-name (string)
     *
     * An identifier for the assumed role session. Typically, you pass the name or identifier that is associated with
     * the user who is using your application. That way, the temporary security credentials that your application will
     * use are associated with that user.
     *
     * This session name is included as part of the ARN and assumed role ID in the AssumedRoleUser response element.
     *
     * The regular expression used to validate this parameter is a string of characters consisting of upper and lower
     * case alphanumeric characters with no spaces. You can also include underscores or any of the following
     * characters: =,.@- .
     * </code>
     * <p>This regular expression enforces a minimum length of 8 and a maximum length of 64 characters.</p>
     */
    private static final String WEB_IDENTITY_ROLE_SESSION_NAME_REGEX =
                                                "^[A-Za-z][A-Za-z0-9_=,.@-]{" +
                                                (MINIMUM_WEB_IDENTITY_ROLE_SESSION_NAME_LENGTH - 1) + "," +
                                                (MAXIMUM_WEB_IDENTITY_ROLE_SESSION_NAME_LENGTH - 1) + "}$";

    static {
        try {
            final Pattern pattern = Pattern.compile(WEB_IDENTITY_ROLE_SESSION_NAME_REGEX);

            WEB_IDENTITY_ROLE_SESSION_NAME_PATTERN = pattern;
        } catch (final PatternSyntaxException pse) {
            final ExceptionInInitializerError error =
                    new ExceptionInInitializerError("Failed to compile the regular expression \"" +
                                                    pse.getPattern() + "\" at index " +
                                                    pse.getIndex() + ". The error is " +
                                                    pse.getMessage() + ".");
            error.initCause(pse);
            throw error;
        }
    }

    /**
     * Validate the Web Identity Role Session Name.
     *
     * <p>The AWS Role Session name is optional, so this method needs to override the super class method.</p>
     *
     * <p>This method is called indirectly by the
     * {@code org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsProviderFactory.validate(ValidationContext)}
     * method, which calls the {@code validate(ValidationContext)} method on each
     * class in the {@code org.apache.nifi.processors.aws.credentials.provider.factory.strategies} package.</p>
     *
     * <p>This means that the value of the {@link PropertyDescriptor} might not be set, in which case, the value
     * must not be validated.</p>
     *
     * @param webIdentityRoleSessionNameProperty
     *          The {@link PropertyDescriptor} for the Web Identity Role Session Name property to validate.
     * @param validationContext
     *          The {@link ValidationContext} to validate against.
     * @param validationFailureResults
     *          The {@link Collection} of failed validations.
     */
//    @Override
//    public void validate(final PropertyDescriptor webIdentityRoleSessionNameProperty,
//                         final ValidationContext validationContext,
//                         final Collection<ValidationResult> validationFailureResults) {
//
//        final PropertyValue propertyValue = validationContext.getProperty(webIdentityRoleSessionNameProperty);
//
//        final String subject = webIdentityRoleSessionNameProperty.getDisplayName();
//        ValidationResult validationResult = validatePropertyPresent(subject, propertyValue);
//
//        boolean isValid = validationResult.isValid();
//        if (isValid) {
//            if (propertyValue.isSet()) {
//                final String input = propertyValue.getValue();
//                validationResult = validate(subject, input, validationContext);
//            }
//        }
//
//        if (!validationResult.isValid()) {
//            validationFailureResults.add(validationResult);
//        }
//    }

    /**
     * Build the description from the specified subject and input.
     * @param subject
     *          What is being being validated.
     * @param input
     *          The value being validated.
     * @return
     *          The description.
     */
    private String buildExplanation(final String subject, final String input) {
        final StringBuilder builder = new StringBuilder(2048);

        builder.append(input).append("\" is not a valid ").append(subject).append("\n")
               .append("A web identity role session name must be between ")
               .append(MINIMUM_WEB_IDENTITY_ROLE_SESSION_NAME_LENGTH)
               .append(" and ")
               .append(MAXIMUM_WEB_IDENTITY_ROLE_SESSION_NAME_LENGTH)
               .append(" characters long.\n")
               .append("It must not contain space or tab characters.\n")
               .append("It must start with an alphabetic character.\n")
               .append("The remaining characters can be alphanumeric, '_', '=', ',', '.', '@', or '-'.\n");
        return builder.toString();
    }

    /**
     * @return {@code true} if this property is mandatory.
     */
    @Override
    protected boolean isMandatoryProperty() {
        return false;
    }

    /**
     * Validate the AWS Web Identity Role Session Name.
     *
     * @param subject
     *          What is being validated.
     * @param input
     *          The string (value) to be validated.
     * @return
     *          A {@link ValidationResult} object containing the outcome of the validation.
     *          {@link ValidationResult#isValid()} will return {@code true} if the the AWS Web Identity Role Session
     *          name is OK, or {@code false} if validation fails.
     */
    @Override
    protected ValidationResult validateValue(final String subject, final String input) {

        // Validate the Web Identity Role Session Name value against the regular expression.
        final Matcher matcher = WEB_IDENTITY_ROLE_SESSION_NAME_PATTERN.matcher(evaluatedInput);
        final boolean isValid = matcher.matches();
        String explanation = null;
        if (!isValid) {
            explanation = buildExplanation(subject, input);
        }
        return new ValidationResult.Builder().subject(subject)
                                             .input(isValid ? null : input)
                                             .valid(isValid)
                                             .explanation(explanation)
                                             .build();
    }
}
