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

package org.radarcns.android.device;

import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.IBinder;
import androidx.annotation.NonNull;

import org.radarcns.android.MainActivity;
import org.radarcns.android.kafka.ServerStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.radarcns.android.device.DeviceService.DEVICE_SERVICE_CLASS;
import static org.radarcns.android.device.DeviceService.DEVICE_STATUS_CHANGED;
import static org.radarcns.android.device.DeviceService.DEVICE_STATUS_NAME;
import static org.radarcns.android.device.DeviceService.SERVER_RECORDS_SENT_NUMBER;
import static org.radarcns.android.device.DeviceService.SERVER_RECORDS_SENT_TOPIC;
import static org.radarcns.android.device.DeviceService.SERVER_STATUS_CHANGED;

public class DeviceServiceConnection<S extends BaseDeviceState> extends BaseServiceConnection<S> {
    private static final Logger logger = LoggerFactory.getLogger(DeviceServiceConnection.class);
    private final MainActivity mainActivity;

    private final BroadcastReceiver statusReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (intent.getAction().equals(DEVICE_STATUS_CHANGED)) {
                if (getServiceClassName().equals(intent.getStringExtra(DEVICE_SERVICE_CLASS))) {
                    if (intent.hasExtra(DEVICE_STATUS_NAME)) {
                        deviceName = intent.getStringExtra(DEVICE_STATUS_NAME);
                        logger.info("Device status changed of device {}", deviceName);
                    }
                    setDeviceStatus(DeviceStatusListener.Status.values()[intent.getIntExtra(DEVICE_STATUS_CHANGED, 0)]);
                    logger.info("Updated device status to {}", getDeviceStatus());
                    mainActivity.deviceStatusUpdated(DeviceServiceConnection.this, getDeviceStatus());
                }
            }
        }
    };

    private final BroadcastReceiver serverStatusListener = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (intentMatches(intent, SERVER_STATUS_CHANGED)) {
                final ServerStatusListener.Status status = ServerStatusListener.Status.values()[intent.getIntExtra(SERVER_STATUS_CHANGED, 0)];
                mainActivity.updateServerStatus(status);
            } else if (intentMatches(intent, SERVER_RECORDS_SENT_TOPIC)) {
                String topic = intent.getStringExtra(SERVER_RECORDS_SENT_TOPIC); // topicName that updated
                int numberOfRecordsSent = intent.getIntExtra(SERVER_RECORDS_SENT_NUMBER, 0);
                mainActivity.updateServerRecordsSent(DeviceServiceConnection.this, topic, numberOfRecordsSent);
            }
        }
    };

    private boolean intentMatches(Intent intent, String action) {
        return intent.getAction().equals(action)
                && getServiceClassName().equals(intent.getStringExtra(DEVICE_SERVICE_CLASS));
    }

    public DeviceServiceConnection(@NonNull MainActivity mainActivity, String serviceClassName) {
        super(serviceClassName);
        this.mainActivity = mainActivity;
    }

    @Override
    public void onServiceConnected(final ComponentName className,
                                   IBinder service) {
        mainActivity.registerReceiver(statusReceiver,
                new IntentFilter(DEVICE_STATUS_CHANGED));

        IntentFilter serverStatusFilter = new IntentFilter();
        serverStatusFilter.addAction(SERVER_STATUS_CHANGED);
        serverStatusFilter.addAction(SERVER_RECORDS_SENT_TOPIC);
        mainActivity.registerReceiver(serverStatusListener, serverStatusFilter);

        if (!hasService()) {
            super.onServiceConnected(className, service);
            mainActivity.serviceConnected(this);
        }
    }

    @Override
    public void onServiceDisconnected(ComponentName className) {
        boolean hadService = hasService();
        super.onServiceDisconnected(className);

        if (hadService) {
            mainActivity.unregisterReceiver(statusReceiver);
            mainActivity.unregisterReceiver(serverStatusListener);
            mainActivity.serviceDisconnected(this);
        }
    }
}
