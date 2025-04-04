/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    public static final int DEFAULT_MAX_ATTEMPTS = 10;
    public static final Duration DEFAULT_MIN_BACKOFF_DELAY = new Duration(1, SECONDS);
    public static final Duration DEFAULT_MAX_BACKOFF_DELAY = new Duration(2, SECONDS);
    public static final Duration DEFAULT_MAX_RETRY_TIME = new Duration(30, SECONDS);
    public static final double DEFAULT_SCALE_FACTOR = 2.0;

    private final int maxAttempts;
    private final Duration minSleepTime;
    private final Duration maxSleepTime;
    private final double scaleFactor;
    private final Duration maxRetryTime;
    private final List<Class<? extends Exception>> stopOnExceptions;

    private RetryDriver(
            int maxAttempts,
            Duration minSleepTime,
            Duration maxSleepTime,
            double scaleFactor,
            Duration maxRetryTime,
            List<Class<? extends Exception>> stopOnExceptions)
    {
        this.maxAttempts = maxAttempts;
        this.minSleepTime = minSleepTime;
        this.maxSleepTime = maxSleepTime;
        this.scaleFactor = scaleFactor;
        this.maxRetryTime = maxRetryTime;
        this.stopOnExceptions = stopOnExceptions;
    }

    private RetryDriver()
    {
        this(DEFAULT_MAX_ATTEMPTS,
                DEFAULT_MIN_BACKOFF_DELAY,
                DEFAULT_MAX_BACKOFF_DELAY,
                DEFAULT_SCALE_FACTOR,
                DEFAULT_MAX_RETRY_TIME,
                ImmutableList.of());
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    public final RetryDriver maxAttempts(int maxAttempts)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, stopOnExceptions);
    }

    public final RetryDriver exponentialBackoff(Duration minSleepTime, Duration maxSleepTime, Duration maxRetryTime, double scaleFactor)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, stopOnExceptions);
    }

    @SafeVarargs
    public final RetryDriver stopOn(Class<? extends Exception>... classes)
    {
        requireNonNull(classes, "classes is null");
        List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(stopOnExceptions)
                .addAll(Arrays.asList(classes))
                .build();

        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptions);
    }

    public RetryDriver stopOnIllegalExceptions()
    {
        return stopOn(NullPointerException.class, IllegalStateException.class, IllegalArgumentException.class);
    }

    public <V> V run(String callableName, Callable<V> callable)
            throws Exception
    {
        requireNonNull(callableName, "callableName is null");
        requireNonNull(callable, "callable is null");

        List<Throwable> suppressedExceptions = new ArrayList<>();
        long startTime = System.nanoTime();
        int attempt = 0;
        while (true) {
            attempt++;

            try {
                return callable.call();
            }
            catch (Exception e) {
                // Immediately stop retry attempts once an interrupt has been received
                if (e instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                    addSuppressed(e, suppressedExceptions);
                    throw e;
                }
                for (Class<? extends Exception> clazz : stopOnExceptions) {
                    if (clazz.isInstance(e)) {
                        addSuppressed(e, suppressedExceptions);
                        throw e;
                    }
                }
                if (attempt >= maxAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0) {
                    addSuppressed(e, suppressedExceptions);
                    throw e;
                }
                log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());

                suppressedExceptions.add(e);

                int delayInMs = (int) Math.min(minSleepTime.toMillis() * Math.pow(scaleFactor, attempt - 1), maxSleepTime.toMillis());
                int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayInMs * 0.1)));
                try {
                    TimeUnit.MILLISECONDS.sleep(delayInMs + jitter);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    Exception exception = new RuntimeException(ie);
                    addSuppressed(exception, suppressedExceptions);
                    throw exception;
                }
            }
        }
    }

    private static void addSuppressed(Exception exception, List<Throwable> suppressedExceptions)
    {
        for (Throwable suppressedException : suppressedExceptions) {
            if (exception != suppressedException) {
                exception.addSuppressed(suppressedException);
            }
        }
    }
}
