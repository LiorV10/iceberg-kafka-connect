/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.data;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RESTClient} that wraps a delegate client and retries REST catalog operations on
 * transient failures, using exponential backoff with jitter.
 *
 * <p>This is intended for deployments where the REST catalog is unstable (for example, returns
 * intermittent {@code 5xx} responses because its backing database is flaky) and the default
 * Iceberg HTTP client retry strategy does not retry enough operations.
 *
 * <p><strong>Safety note on commits:</strong> {@code post} and {@code postForm} are non-idempotent.
 * In particular, the table commit is a {@code POST}. Retrying a commit whose outcome is unknown
 * (for example, after the response was lost) can double-apply data files and create duplicate rows.
 * For that reason, non-idempotent operations are only retried when {@code retryAllMethods} is
 * {@code true}. The default is {@code false} (retry idempotent operations only), which is the safe
 * choice.
 */
public class RetryingRESTClient implements RESTClient {

  private static final Logger LOG = LoggerFactory.getLogger(RetryingRESTClient.class);

  private final RESTClient delegate;
  private final int maxRetries;
  private final long baseBackoffMs;
  private final long maxBackoffMs;
  private final boolean retryAllMethods;

  public RetryingRESTClient(
      RESTClient delegate,
      int maxRetries,
      long baseBackoffMs,
      long maxBackoffMs,
      boolean retryAllMethods) {
    this.delegate = delegate;
    this.maxRetries = maxRetries;
    this.baseBackoffMs = baseBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.retryAllMethods = retryAllMethods;
  }

  @FunctionalInterface
  private interface Action<T> {
    T run();
  }

  private <T> T withRetry(String opName, boolean idempotent, Action<T> action) {
    // Non-idempotent operations (POST, including the commit) are only retried when explicitly
    // enabled, because retrying an uncertain commit can double-apply data.
    boolean retryable = idempotent || retryAllMethods;

    int attempt = 0;
    RuntimeException lastError = null;
    while (true) {
      try {
        return action.run();
      } catch (RuntimeException e) {
        lastError = e;

        if (!retryable) {
          LOG.debug(
              "Not retrying non-idempotent op {} (retryAllMethods=false): {}", opName, e.toString());
          throw e;
        }

        if (!isTransient(e) || attempt >= maxRetries) {
          throw e;
        }

        long backoff = Math.min(maxBackoffMs, baseBackoffMs * (1L << attempt));
        long jitter = backoff <= 0 ? 0 : ThreadLocalRandom.current().nextLong(1 + backoff / 10);
        long sleepMs = backoff + jitter;
        LOG.warn(
            "Retrying REST op {} (attempt {} of {}) after {} ms due to: {}",
            opName, attempt + 1, maxRetries, sleepMs, e.toString());
        sleep(sleepMs);
        attempt++;
      }
    }
  }

  private boolean isTransient(RuntimeException e) {
    // 5xx server failures and IO/timeout failures surfaced by the Iceberg REST client.
    if (e instanceof ServiceFailureException) {
      return true;
    }
    if (e instanceof ServiceUnavailableException) {
      return true;
    }
    if (e instanceof RESTException) {
      return true;
    }
    // SQL-layer failures surfaced by the REST catalog (for example UncheckedSQLException) arrive
    // as a server error; match by simple name to avoid a hard dependency on the JDBC class.
    Throwable current = e;
    while (current != null) {
      String simpleName = current.getClass().getSimpleName();
      if (simpleName.contains("UncheckedSQLException") || simpleName.contains("SQLException")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void sleep(long ms) {
    if (ms <= 0) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RESTException(ie, "Interrupted during REST retry backoff");
    }
  }

  // ---- RESTClient: concrete (non-default) methods ----

  @Override
  public void head(
      String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    withRetry(
        "HEAD " + path,
        true,
        () -> {
          delegate.head(path, headers, errorHandler);
          return null;
        });
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return withRetry(
        "DELETE " + path,
        true,
        () -> delegate.delete(path, responseType, headers, errorHandler));
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return withRetry(
        "GET " + path,
        true,
        () -> delegate.get(path, queryParams, responseType, headers, errorHandler));
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    // NON-IDEMPOTENT: includes the table commit. Only retried when retryAllMethods=true.
    return withRetry(
        "POST " + path,
        false,
        () -> delegate.post(path, body, responseType, headers, errorHandler));
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    // NON-IDEMPOTENT (form post, for example OAuth token). Only retried when retryAllMethods=true.
    return withRetry(
        "POSTFORM " + path,
        false,
        () -> delegate.postForm(path, formData, responseType, headers, errorHandler));
  }

  @Override
  public RESTClient withAuthSession(AuthSession session) {
    return new RetryingRESTClient(
        delegate.withAuthSession(session), maxRetries, baseBackoffMs, maxBackoffMs, retryAllMethods);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
