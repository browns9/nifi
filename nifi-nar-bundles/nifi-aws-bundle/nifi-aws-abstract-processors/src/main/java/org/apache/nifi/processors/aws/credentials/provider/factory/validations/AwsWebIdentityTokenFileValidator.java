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

import org.apache.nifi.components.ValidationResult;


/**
 * Class to validate an AWS Web Identity Token File.
 *
 * @author Steve Brown, Estafet Ltd.
 *
 */
public final class AwsWebIdentityTokenFileValidator extends AbstractPropertyValidator {

    /**
     * @return {@code true} if this property is mandatory.
     */
    @Override
    protected boolean isMandatoryProperty() {
        return true;
    }

    /**
     * Validate the AWS Web Identity Token File path.
     *
     * @param subject
     *          What is being validated.
     * @param input
     *          The string (value) to be validated.
     * @return
     *          A {@link ValidationResult} object containing the outcome of the validation.
     *          {@link ValidationResult#isValid()} will return {@code true} if the the AWS Web Identity Token File path
     *          is OK, or {@code false} if validation fails.
     */
    @Override
    protected ValidationResult validateValue(final String subject, final String input) {
        Path path = Paths.get(evaluatedInput);

        String explanation = null;
        boolean isValid = true;
        try {
            path = path.toRealPath();

            isValid = Files.isRegularFile(path);

            explanation = "the path + " + path.toString() + " does not exist, or is not a regular file, " +
                          "or it cannot be determined whether the file is a regular file or not.";
        } catch (final IOException ioe) {
            explanation = "an error occurred getting the real path from " + evaluatedInput + ". " +
                          "This can happen if the effective user does not have permissions to " +
                          "access all the components of the path. The error is\n" +
                           ioe.getMessage() + "\n";
            isValid = false;
        }
        return new ValidationResult.Builder().subject(subject)
                                             .input(isValid ? null : input)
                                             .valid(isValid)
                                             .explanation(explanation)
                                             .build();
    }
}
