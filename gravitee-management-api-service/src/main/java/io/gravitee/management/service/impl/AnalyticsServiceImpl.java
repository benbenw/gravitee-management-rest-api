/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.management.service.impl;

import io.gravitee.management.model.analytics.Bucket;
import io.gravitee.management.model.analytics.HealthAnalytics;
import io.gravitee.management.model.analytics.HistogramAnalytics;
import io.gravitee.management.service.AnalyticsService;
import io.gravitee.repository.analytics.api.AnalyticsRepository;
import io.gravitee.repository.analytics.query.*;
import io.gravitee.repository.analytics.query.response.HealthResponse;
import io.gravitee.repository.analytics.query.response.histogram.Data;
import io.gravitee.repository.analytics.query.response.histogram.HistogramResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
@Component
public class AnalyticsServiceImpl implements AnalyticsService {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(AnalyticsServiceImpl.class);

    @Autowired
    private AnalyticsRepository analyticsRepository;

    @Override
    public HistogramAnalytics apiHits(String apiId, long from, long to, long interval) {
        return apiHits(HitsByApiQuery.Type.HITS, apiId, from, to, interval);
    }

    @Override
    public HistogramAnalytics apiHitsByStatus(String apiId, long from, long to, long interval) {
        return apiHits(HitsByApiQuery.Type.HITS_BY_STATUS, apiId, from, to, interval);
    }

    @Override
    public HistogramAnalytics apiHitsByLatency(String apiId, long from, long to, long interval) {
        final HistogramAnalytics histogramAnalytics = apiHits(HitsByApiQuery.Type.HITS_BY_LATENCY, apiId, from, to, interval);
        return aggregateBuckets(histogramAnalytics);
    }

    @Override
    public HistogramAnalytics apiHitsByApiKey(String apiId, long from, long to, long interval) {
        return apiHits(HitsByApiQuery.Type.HITS_BY_APIKEY, apiId, from, to, interval);
    }

    @Override
    public HistogramAnalytics apiHitsByPayloadSize(String apiId, long from, long to, long interval) {
        final HistogramAnalytics histogramAnalytics = apiHits(HitsByApiQuery.Type.HITS_BY_PAYLOAD_SIZE, apiId, from, to, interval);
        return aggregateBuckets(histogramAnalytics);
    }

    @Override
    public HistogramAnalytics apiKeyHits(String apiKey, long from, long to, long interval) {
        return apiKeyHits(HitsByApiKeyQuery.Type.HITS, apiKey, from, to, interval);
    }

    @Override
    public HistogramAnalytics apiKeyHitsByStatus(String apiKey, long from, long to, long interval) {
        return apiKeyHits(HitsByApiKeyQuery.Type.HITS_BY_STATUS, apiKey, from, to, interval);
    }

    @Override
    public HistogramAnalytics apiKeyHitsByLatency(String apiKey, long from, long to, long interval) {
        return apiKeyHits(HitsByApiKeyQuery.Type.HITS_BY_LATENCY, apiKey, from, to, interval);
    }

    @Override
    public HealthAnalytics health(String api, long from, long to, long interval) {
        logger.debug("Run health query for API '{}'", api);

        try {
            return convert(analyticsRepository.query(api, interval, from, to));
        } catch (Exception ex) {
            logger.error("An unexpected error occurs while searching for health data.", ex);
            return null;
        }
    }

    private HistogramAnalytics apiKeyHits(HitsByApiKeyQuery.Type type, String apiKey, long from, long to, long interval) {
        logger.debug("Run analytics query {} for API key '{}'", type, apiKey);

        try {
            return runHistoricalQuery(QueryBuilders.query()
                    .hitsByApiKey(apiKey)
                    .period(DateRangeBuilder.between(from, to))
                    .interval(IntervalBuilder.interval(interval))
                    .type(type)
                    .build(), from, interval);

        } catch (Exception ex) {
            logger.error("An unexpected error occurs while searching for analytics data.", ex);
            return null;
        }
    }

    private HistogramAnalytics apiHits(HitsByApiQuery.Type type, String apiId, long from, long to, long interval) {
        logger.debug("Run analytics query {} for API '{}'", type, apiId);

        try {
            return runHistoricalQuery(QueryBuilders.query()
                    .hitsByApi(apiId)
                    .period(DateRangeBuilder.between(from, to))
                    .interval(IntervalBuilder.interval(interval))
                    .type(type)
                    .build(), from, interval);

        } catch (Exception ex) {
            logger.error("An unexpected error occurs while searching for analytics data.", ex);
            return null;
        }
    }

    private HistogramAnalytics runHistoricalQuery(Query<HistogramResponse> query, long from, long interval) throws Exception {
        return convert(analyticsRepository.query(query), from, interval);
    }

    private HistogramAnalytics convert(HistogramResponse histogramResponse, long from, long interval) {
        HistogramAnalytics analytics = new HistogramAnalytics();

        analytics.setTimestamps(histogramResponse.timestamps());

        for (io.gravitee.repository.analytics.query.response.histogram.Bucket bucket : histogramResponse.values()) {
            Bucket analyticsBucket = convertBucket(analytics.getTimestamps(), from, interval, bucket);
            analytics.getValues().add(analyticsBucket);
        }
        return analytics;
    }

    private Bucket convertBucket(List<Long> timestamps, long from, long interval, io.gravitee.repository.analytics.query.response.histogram.Bucket bucket) {
        Bucket analyticsBucket = new Bucket();
        analyticsBucket.setName(bucket.name());

        for (io.gravitee.repository.analytics.query.response.histogram.Bucket childBucket : bucket.buckets()) {
            analyticsBucket.getBuckets().add(convertBucket(timestamps, from, interval, childBucket));
        }

        for (Map.Entry<String, List<Data>> dataBucket : bucket.data().entrySet()) {
            Bucket analyticsDataBucket = new Bucket();
            analyticsDataBucket.setName(dataBucket.getKey());

            long[] values = new long[timestamps.size()];
            for (Data data : dataBucket.getValue()) {
                values[(int) ((data.timestamp() - from) / interval)] = data.count();
            }

            analyticsDataBucket.setData(values);

            analyticsBucket.getBuckets().add(analyticsDataBucket);
        }

        return analyticsBucket;
    }

    private HealthAnalytics convert(HealthResponse response) {
        HealthAnalytics healthAnalytics = new HealthAnalytics();

        healthAnalytics.setTimestamps(response.timestamps());
        healthAnalytics.setBuckets(response.buckets());

        return healthAnalytics;
    }

    private HistogramAnalytics aggregateBuckets(final HistogramAnalytics histogramAnalytics) {
        if (histogramAnalytics.getValues() != null && !histogramAnalytics.getValues().isEmpty()) {
            final Bucket bucket = histogramAnalytics.getValues().get(0);

            final Optional<Bucket> optBucketAverage = bucket.getBuckets().stream()
                    .reduce((bucket1, bucket2) -> {
                        final Bucket newBucket = new Bucket();
                        newBucket.setName("Average");

                        final long[] bucketData = bucket1.getData();
                        final long[] newData = new long[bucketData.length];
                        for (int i = 0; i < bucketData.length; i++) {
                            if (bucketData[i] == 0) {
                                newData[i] = bucket2.getData()[i];
                            } else if (bucket2.getData()[i] == 0) {
                                newData[i] = bucketData[i];
                            } else {
                                newData[i] = (bucketData[i] + bucket2.getData()[i]) / 2;
                            }
                        }
                        newBucket.setData(newData);
                        return newBucket;
                    });

            final Optional<Bucket> optBucketMin = bucket.getBuckets().stream()
                    .reduce((bucket1, bucket2) -> {
                        final Bucket newBucket = new Bucket();
                        newBucket.setName("Min");

                        final long[] bucketData = bucket1.getData();
                        final long[] newData = new long[bucketData.length];
                        for (int i = 0; i < bucketData.length; i++) {
                            if (bucketData[i] == 0) {
                                newData[i] = bucket2.getData()[i];
                            } else if (bucket2.getData()[i] == 0) {
                                newData[i] = bucketData[i];
                            } else {
                                newData[i] = Math.min(bucketData[i], bucket2.getData()[i]);
                            }
                        }
                        newBucket.setData(newData);
                        return newBucket;
                    });

            final Optional<Bucket> optBucketMax = bucket.getBuckets().stream()
                    .reduce((bucket1, bucket2) -> {
                        final Bucket newBucket = new Bucket();
                        newBucket.setName("Max");

                        final long[] bucketData = bucket1.getData();
                        final long[] newData = new long[bucketData.length];
                        for (int i = 0; i < bucketData.length; i++) {
                            newData[i] = Math.max(bucketData[i], bucket2.getData()[i]);
                        }
                        newBucket.setData(newData);
                        return newBucket;
                    });

            final List<Bucket> buckets = new ArrayList<>(3);
            if (optBucketAverage.isPresent()) {
                buckets.add(optBucketAverage.get());
            }
            if (optBucketMin.isPresent()) {
                buckets.add(optBucketMin.get());
            }
            if (optBucketMax.isPresent()) {
                buckets.add(optBucketMax.get());
            }
            bucket.setBuckets(buckets);
        }
        return histogramAnalytics;
    }
}
