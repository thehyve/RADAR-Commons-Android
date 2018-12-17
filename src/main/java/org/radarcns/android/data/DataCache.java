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

import android.os.Parcel;
import android.util.Pair;

import org.radarcns.data.Record;
import org.radarcns.topic.AvroTopic;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;

public interface DataCache<K, V> extends Flushable, Closeable {
    /**
     * Get all unsent records in the cache.
     *
     * @return records.
     */
    List<Record<K, V>> unsentRecords(int limit, long sizeLimit) throws IOException;

    /**
     * Get latest records in the cache, from new to old.
     *
     * @return records.
     */
    List<Record<K, V>> getRecords(int limit) throws IOException;

    /**
     * Get a pair with the number of [unsent records], [sent records]
     */
    Pair<Long, Long> numberOfRecords();

    /**
     * Remove all records before a given offset.
     * @param offset offset (inclusive) to remove.
     * @return number of rows removed
     */
    int markSent(long offset) throws IOException;

    /** Add a new measurement to the cache. */
    void addMeasurement(K key, V value);

    /**
     * Remove all sent records before a given time.
     * @param millis time in milliseconds before which to remove.
     * @return number of rows removed
     */
    int removeBeforeTimestamp(long millis);

    /** Get the topic the cache stores. */
    AvroTopic<K, V> getTopic();

    /**
     * Write the latest records in the cache to a parcel, from new to old.
     */
    void writeRecordsToParcel(Parcel dest, int limit) throws IOException;

    /** Return a list to cache. It will be cleared immediately and should not be used again. */
    void returnList(List list);

    /** Set the time until data is committed to disk. Time in milliseconds. */
    void setTimeWindow(long period);

    /** Set the maximum size the data cache may have in bytes. */
    void setMaximumSize(int bytes);
}
