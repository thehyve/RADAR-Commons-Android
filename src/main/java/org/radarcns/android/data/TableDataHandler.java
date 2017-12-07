/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.android.data;

import android.content.Context;
import android.os.Process;
import android.support.annotation.NonNull;
import okhttp3.Headers;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.android.auth.AppAuthState;
import org.radarcns.android.kafka.KafkaDataSubmitter;
import org.radarcns.android.kafka.ServerStatusListener;
import org.radarcns.android.util.*;
import org.radarcns.config.ServerConfig;
import org.radarcns.data.SpecificRecordEncoder;
import org.radarcns.key.MeasurementKey;
import org.radarcns.producer.rest.RestSender;
import org.radarcns.producer.rest.SchemaRetriever;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stores data in databases and sends it to the server.
 */
public class TableDataHandler implements DataHandler<MeasurementKey, SpecificRecord>, BatteryLevelReceiver.BatteryLevelListener, NetworkConnectedReceiver.NetworkConnectedListener {
    private static final Logger logger = LoggerFactory.getLogger(TableDataHandler.class);

    public static final long DATA_RETENTION_DEFAULT = 86400000L;
    public static final int SEND_LIMIT_DEFAULT = 1000;
    public static final long UPLOAD_RATE_DEFAULT = 10L;
    public static final long SENDER_CONNECTION_TIMEOUT_DEFAULT = 10L;
    public static final float MINIMUM_BATTERY_LEVEL = 0.1f;
    public static final float REDUCED_BATTERY_LEVEL = 0.2f;

    private final Map<AvroTopic<MeasurementKey, ? extends SpecificRecord>, DataCache<MeasurementKey, ? extends SpecificRecord>> tables;
    private final Set<ServerStatusListener> statusListeners;
    private final SingleThreadExecutorFactory executorFactory;
    private final BatteryLevelReceiver batteryLevelReceiver;
    private final NetworkConnectedReceiver networkConnectedReceiver;
    private final AtomicBoolean sendOnlyWithWifi;
    private final Context context;
    private AppAuthState authState;
    private ServerConfig kafkaConfig;
    private SchemaRetriever schemaRetriever;
    private int kafkaRecordsSendLimit;
    private final AtomicLong dataRetention;
    private long kafkaUploadRate;
    private long senderConnectionTimeout;

    private ServerStatusListener.Status status;
    private Map<String, Integer> lastNumberOfRecordsSent = new TreeMap<>();
    private KafkaDataSubmitter<SpecificRecord> submitter;
    private RestSender<MeasurementKey, SpecificRecord> sender;
    private final AtomicFloat minimumBatteryLevel;
    private boolean useCompression;

    /**
     * Create a data handler. If kafkaConfig is null, data will only be stored to disk, not uploaded.
     */
    public TableDataHandler(Context context, ServerConfig kafkaUrl, SchemaRetriever schemaRetriever,
                            List<AvroTopic<MeasurementKey, ? extends SpecificRecord>> topics,
                            int maxBytes, boolean sendOnlyWithWifi, AppAuthState authState)
            throws IOException {
        this.context = context;
        this.kafkaConfig = kafkaUrl;
        this.schemaRetriever = schemaRetriever;
        this.kafkaUploadRate = UPLOAD_RATE_DEFAULT;
        this.kafkaRecordsSendLimit = SEND_LIMIT_DEFAULT;
        this.senderConnectionTimeout = SENDER_CONNECTION_TIMEOUT_DEFAULT;
        this.minimumBatteryLevel = new AtomicFloat(MINIMUM_BATTERY_LEVEL);
        this.executorFactory = new SharedSingleThreadExecutorFactory(
                new AndroidThreadFactory("TableDataHandler", Process.THREAD_PRIORITY_BACKGROUND));

        this.batteryLevelReceiver = new BatteryLevelReceiver(context, this);
        this.networkConnectedReceiver = new NetworkConnectedReceiver(context, this);
        this.sendOnlyWithWifi = new AtomicBoolean(sendOnlyWithWifi);
        this.useCompression = false;
        this.authState = authState;

        tables = new HashMap<>(topics.size() * 2);
        for (AvroTopic<MeasurementKey, ? extends SpecificRecord> topic : topics) {
            DataCache<MeasurementKey, ? extends SpecificRecord> cache = CacheStore.getInstance()
                    .getOrCreateCache(context.getApplicationContext(), topic);
            cache.setMaximumSize(maxBytes);
            tables.put(topic, cache);
        }
        dataRetention = new AtomicLong(DATA_RETENTION_DEFAULT);

        submitter = null;
        sender = null;

        statusListeners = new HashSet<>();

        if (kafkaUrl != null) {
            doEnableSubmitter();
        } else {
            updateServerStatus(Status.DISABLED);
        }
    }

    private synchronized void updateUploadRate() {
        if (submitter != null) {
            submitter.setUploadRate(getPreferredUploadRate());
        }
    }

    private long getPreferredUploadRate() {
        if (batteryLevelReceiver.hasMinimumLevel(REDUCED_BATTERY_LEVEL)) {
            return kafkaUploadRate;
        } else {
            return kafkaUploadRate * 5;
        }
    }

    /**
     * Start submitting data to the server.
     *
     * This will not do anything if there is not already a submitter running, if it is disabled,
     * if the network is not connected or if the battery is running too low.
     */
    public synchronized void start() {
        if (isStarted()
                || status == Status.DISABLED
                || !networkConnectedReceiver.hasConnection(sendOnlyWithWifi.get())
                || !batteryLevelReceiver.hasMinimumLevel(minimumBatteryLevel.get())) {
            return;
        }

        updateServerStatus(Status.CONNECTING);
        this.sender = new RestSender.Builder<MeasurementKey, SpecificRecord>()
                .server(kafkaConfig)
                .schemaRetriever(schemaRetriever)
                .encoders(new SpecificRecordEncoder(false), new SpecificRecordEncoder(false))
                .connectionTimeout(senderConnectionTimeout, TimeUnit.SECONDS)
                .useCompression(useCompression)
                .headers(authState.getHeaders())
                .build();
        this.submitter = new KafkaDataSubmitter<>(context, this, sender, kafkaRecordsSendLimit,
                getPreferredUploadRate(), authState.getUserId());
    }

    public synchronized boolean isStarted() {
        return submitter != null;
    }

    /**
     * Pause sending any data.
     * This waits for any remaining data to be sent.
     */
    public synchronized void stop() {
        if (submitter != null) {
            this.submitter.close();
            this.submitter = null;
            this.sender = null;
        }
        if (status != Status.DISABLED) {
            updateServerStatus(Status.READY);
        }
    }

    /** Do not submit any data, only cache it. If it is already disabled, this does nothing. */
    public synchronized void disableSubmitter() {
        if (status != Status.DISABLED) {
            updateServerStatus(Status.DISABLED);
            if (isStarted()) {
                stop();
            }
            try {
                networkConnectedReceiver.unregister();
                batteryLevelReceiver.unregister();
            } catch (IllegalArgumentException ex) {
                logger.warn("Not yet registered");
            }
        }
    }

    /** Start submitting data. If it is already submitting data, this does nothing. */
    public synchronized void enableSubmitter() {
        if (status == Status.DISABLED) {
            doEnableSubmitter();
            start();
        }
    }

    private void doEnableSubmitter() {
        try {
            networkConnectedReceiver.register();
            batteryLevelReceiver.register();
        } catch (IllegalArgumentException ex) {
            logger.warn("Not yet registered");
        }
        updateServerStatus(Status.READY);
    }

    /**
     * Sends any remaining data and closes the tables and connections.
     * @throws IOException if the tables cannot be flushed
     */
    public synchronized void close() throws IOException {
        if (status != Status.DISABLED) {
            try {
                networkConnectedReceiver.unregister();
                batteryLevelReceiver.unregister();
            } catch (IllegalArgumentException ex) {
                logger.warn("Not yet registered");
            }
        }
        if (this.submitter != null) {
            this.submitter.close();  // will also close sender
            this.submitter = null;
            this.sender = null;
        }
        if (schemaRetriever != null) {
            schemaRetriever.close();
        }
        clean();
        for (DataCache<MeasurementKey, ? extends SpecificRecord> table : tables.values()) {
            table.close();
        }
        executorFactory.close();
    }

    @Override
    public void clean() {
        long timestamp = (System.currentTimeMillis() - dataRetention.get());
        for (DataCache<MeasurementKey, ? extends SpecificRecord> table : tables.values()) {
            table.removeBeforeTimestamp(timestamp);
        }
    }

    /**
     * Try to submit given data. This will only send data if the submitter is active and there is
     * a connection with the server. Otherwise, the data is discarded.
     */
    public synchronized boolean trySend(AvroTopic topic, long offset, MeasurementKey deviceId,
                                        SpecificRecord record) {
        return submitter != null && submitter.trySend(topic, offset, deviceId, record);
    }

    /**
     * Check the connection with the server.
     *
     * Updates will be given to any listeners registered to
     * {@link #addStatusListener(ServerStatusListener)}.
     */
    public synchronized void checkConnection() {
        if (status == Status.DISABLED) {
            return;
        }
        if (!isStarted()) {
            start();
        }
        if (isStarted()) {
            submitter.checkConnection();
        }
    }

    /**
     * Get the table of a given topic
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V extends SpecificRecord> DataCache<MeasurementKey, V> getCache(AvroTopic<MeasurementKey, V> topic) {
        return (DataCache<MeasurementKey, V>)this.tables.get(topic);
    }

    @Override
    public <V extends SpecificRecord> void addMeasurement(DataCache<MeasurementKey, V> table, MeasurementKey key, V value) {
        table.addMeasurement(key, value);
    }

    @Override
    public void setMaximumCacheSize(int numBytes) {
        for (DataCache cache : tables.values()) {
            cache.setMaximumSize(numBytes);
        }
    }

    public Map<AvroTopic<MeasurementKey, ? extends SpecificRecord>, DataCache<MeasurementKey, ? extends SpecificRecord>> getCaches() {
        return tables;
    }

    /** Add a listener for ServerStatus updates. */
    public void addStatusListener(ServerStatusListener listener) {
        synchronized (statusListeners) {
            statusListeners.add(listener);
        }
    }
    /** Remove a listener for ServerStatus updates. */
    public void removeStatusListener(ServerStatusListener listener) {
        synchronized (statusListeners) {
            statusListeners.remove(listener);
        }
    }

    @Override
    public void updateServerStatus(ServerStatusListener.Status status) {
        synchronized (statusListeners) {
            for (ServerStatusListener listener : statusListeners) {
                listener.updateServerStatus(status);
            }
            this.status = status;
        }
    }

    /** Get the latest server status. */
    public ServerStatusListener.Status getStatus() {
        synchronized (statusListeners) {
            return this.status;
        }
    }

    @Override
    public void updateRecordsSent(String topicName, int numberOfRecords) {
        synchronized (statusListeners) {
            for (ServerStatusListener listener : statusListeners) {
                listener.updateRecordsSent(topicName, numberOfRecords);
            }
            // Overwrite key-value if exists. Only stores the last
            this.lastNumberOfRecordsSent.put(topicName, numberOfRecords );

            if (numberOfRecords < 0 ) {
                String message = String.format(Locale.US, "%1$45s has FAILED uploading", topicName);
                logger.warn(message);
            } else {
                String message = String.format(Locale.US, "%1$45s uploaded %2$4d records",
                        topicName, numberOfRecords);
                logger.info(message);
            }
        }
    }

    public Map<String, Integer> getRecordsSent() {
        synchronized (statusListeners) {
            return this.lastNumberOfRecordsSent;
        }
    }

    public void setDatabaseCommitRate(long period) {
        for (DataCache<?, ?> table : tables.values()) {
            table.setTimeWindow(period);
        }
    }

    public synchronized void setKafkaRecordsSendLimit(int kafkaRecordsSendLimit) {
        if (submitter != null) {
            submitter.setSendLimit(kafkaRecordsSendLimit);
        }
        this.kafkaRecordsSendLimit = kafkaRecordsSendLimit;
    }

    public synchronized void setKafkaUploadRate(long kafkaUploadRate) {
        this.kafkaUploadRate = kafkaUploadRate;
        updateUploadRate();
    }

    public synchronized void setAuthState(AppAuthState state) {
        this.authState = state;
        if (sender != null) {
            ArrayList<Map.Entry<String, String>> authHeaders = authState.getHeaders();
            Headers.Builder builder = new Headers.Builder();
            if (authHeaders != null) {
                for (Map.Entry<String, String> header : authHeaders) {
                    builder.add(header.getKey(), header.getValue());
                }
            }
            sender.setHeaders(builder.build());
        }
        if (submitter != null) {
            submitter.setUserId(authState.getUserId());
        }
    }

    public synchronized void setSenderConnectionTimeout(long senderConnectionTimeout) {
        if (sender != null) {
            sender.setConnectionTimeout(senderConnectionTimeout);
        }
        this.senderConnectionTimeout = senderConnectionTimeout;
    }

    public synchronized void setKafkaConfig(@NonNull ServerConfig kafkaUrl) {
        if (sender != null) {
            sender.setKafkaConfig(kafkaUrl);
        }
        this.kafkaConfig = kafkaUrl;
    }

    public synchronized void setSchemaRetriever(@NonNull SchemaRetriever schemaRetriever) {
        if (sender != null) {
            sender.setSchemaRetriever(schemaRetriever);
        }
        SchemaRetriever oldSchemaRetriever = this.schemaRetriever;
        this.schemaRetriever = schemaRetriever;
        if (oldSchemaRetriever != null) {
            oldSchemaRetriever.close();
        }
    }

    public synchronized void setCompression(boolean useCompression) {
        if (sender != null) {
            sender.setCompression(useCompression);
        }
        this.useCompression = useCompression;
    }

    public void setDataRetention(long dataRetention) {
        this.dataRetention.set(dataRetention);
    }

    public void setMinimumBatteryLevel(float minimumBatteryLevel) {
        this.minimumBatteryLevel.set(minimumBatteryLevel);
        start();
    }

    public void setSendOnlyWithWifi(boolean sendOnlyWithWifi) {
        this.sendOnlyWithWifi.set(sendOnlyWithWifi);
        // trigger network connection setting update
        this.onNetworkConnectionChanged(
                networkConnectedReceiver.isConnected(),
                networkConnectedReceiver.hasWifiOrEthernet());
    }

    @Override
    public void onNetworkConnectionChanged(boolean isConnected, boolean hasWifiOrEthernet) {
        if (isStarted()) {
            if (!isConnected || (!hasWifiOrEthernet && sendOnlyWithWifi.get())) {
                logger.info("Network was disconnected, stopping data sending");
                stop();
            }
        } else {
            // Just try to start: the start method will not do anything if the parameters
            // are not right.
            start();
        }
    }

    @Override
    public void onBatteryLevelChanged(float level, boolean isPlugged) {
        if (isStarted()) {
            if (level < minimumBatteryLevel.get() && !isPlugged) {
                logger.info("Battery level getting low, stopping data sending");
                stop();
            } else {
                updateUploadRate();
            }
        } else {
            // Just try to start: the start method will not do anything if the parameters
            // are not right.
            start();
        }
    }
}
