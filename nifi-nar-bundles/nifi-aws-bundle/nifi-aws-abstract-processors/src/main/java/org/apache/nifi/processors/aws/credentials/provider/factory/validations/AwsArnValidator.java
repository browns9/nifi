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
     * Validate the ARN.
     * @param subject
     *          What is being validated - i.e. what is the property for.
     * @param input
     *          The value of the property being validated.
     * @return
     *          A {@link ValidationResult} object containing the outcome of the validation.
     *          {@link ValidationResult#isValid()} will return {@code true} if the the ARN is OK, or {@code false} if
     *          validation fails.
     */
    @Override
    protected ValidationResult validateValue(final String subject, final String input) {
        String explanation = null;
        boolean isValid = true;
        try {
            // Validate the ARN.
            @SuppressWarnings("unused")
            final Arn arn = Arn.fromString(evaluatedInput);
        } catch (final IllegalArgumentException iae) {

            isValid = false;
            explanation = "the value \"" + evaluatedInput +
                          "\" is not a valid ARN value. The error message is: " + iae.getMessage();
        }

        return new ValidationResult.Builder().subject(subject)
                                             .input(isValid ? null : input)
                                             .valid(isValid)
                                             .explanation(explanation)
                                             .build();
    }

    /**
     * @return {@code true} if this property is mandatory.
     */
    @Override
    protected boolean isMandatoryProperty() {
        return true;
    }
}
