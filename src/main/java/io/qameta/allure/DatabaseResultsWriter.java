/*
 *  Copyright 2019 Qameta Software OÜ
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.qameta.allure;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.qameta.allure.model.TestResult;
import io.qameta.allure.model.TestResultContainer;
import org.bson.Document;
import org.bson.internal.Base64;

import java.io.InputStream;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Dmitry Minchuk
 */
public class DatabaseResultsWriter implements AllureResultsWriter {

    private final MongoCollection<Document> collection;

    public DatabaseResultsWriter(final String mongoBaseUrl, final String dbName, final String collectionName) {
        MongoClient mongoClient = MongoClients.create(mongoBaseUrl);
        this.collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
    }

    @Override
    public void write(final TestResult testResult) {
        final String testResultName = Objects.isNull(testResult.getUuid())
                ? generateTestResultName()
                : generateTestResultName(testResult.getUuid());
        collection.insertOne(new Document(Base64.encode(testResultName.getBytes()), testResult));
    }

    @Override
    public void write(final TestResultContainer testResultContainer) {
        final String testResultContainerName = Objects.isNull(testResultContainer.getUuid())
                ? generateTestResultContainerName()
                : generateTestResultContainerName(testResultContainer.getUuid());
        collection.insertOne(new Document(Base64.encode(testResultContainerName.getBytes()), testResultContainer));
    }

    @Override
    public void write(final String source, final InputStream attachment) {
        collection.insertOne(new Document(Base64.encode(source.getBytes()), attachment));
    }

    protected static String generateTestResultName() {
        return generateTestResultName(UUID.randomUUID().toString());
    }

    protected static String generateTestResultName(final String uuid) {
        return uuid + AllureConstants.TEST_RESULT_FILE_SUFFIX;
    }

    protected static String generateTestResultContainerName() {
        return generateTestResultContainerName(UUID.randomUUID().toString());
    }

    protected static String generateTestResultContainerName(final String uuid) {
        return uuid + AllureConstants.TEST_RESULT_CONTAINER_FILE_SUFFIX;
    }
}
