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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.android.util.AndroidThreadFactory;
import org.radarcns.android.util.SharedSingleThreadExecutorFactory;
import org.radarcns.android.util.SingleThreadExecutorFactory;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.CountedReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static android.os.Process.THREAD_PRIORITY_BACKGROUND;

public class CacheStore {
    private static final Logger logger = LoggerFactory.getLogger(CacheStore.class);

    private static final String KEY_SCHEMA_EXTENSION = ".key.avsc";
    private static final String VALUE_SCHEMA_EXTENSION = ".value.avsc";

    private static final Object SYNC_OBJECT = new Object();
    private static CacheStore store = null;
    private final SpecificData specificData;

    public static CacheStore getInstance() {
        synchronized (SYNC_OBJECT) {
            if (store == null) {
                store = new CacheStore();
            }
            return store;
        }
    }

    private final Map<String, CountedReference<DataCache>> caches;
    private SingleThreadExecutorFactory cacheExecutorFactory;

    private CacheStore() {
        caches = new HashMap<>();
        cacheExecutorFactory = null;
        specificData = new SpecificData(CacheStore.class.getClassLoader()) {
            @Override
            protected boolean isFloat(Object object) {
                return object instanceof Float
                        && !((Float) object).isNaN()
                        && !((Float) object).isInfinite();
            }
            @Override
            protected boolean isDouble(Object object) {
                return object instanceof Double
                        && !((Double) object).isNaN()
                        && !((Double) object).isInfinite();
            }
        };
    }

    @SuppressWarnings("unchecked")
    public synchronized <K extends SpecificRecord, V extends SpecificRecord> DataCache<K, V>
            getOrCreateCache(Context context, AvroTopic<K, V> topic) throws IOException {

        if (cacheExecutorFactory == null) {
            cacheExecutorFactory = new SharedSingleThreadExecutorFactory(
                    new AndroidThreadFactory("DataCache", THREAD_PRIORITY_BACKGROUND));
        }

        CountedReference<DataCache> ref = caches.get(topic.getName());
        if (ref == null) {
            ref = new CountedReference<DataCache>(
                    new TapeCache<>(context, topic, cacheExecutorFactory, specificData));

            caches.put(topic.getName(), ref);

            String fileBase = context.getCacheDir().getAbsolutePath() + '/' + topic.getName();
            writeSchema(topic.getKeySchema(), new File(fileBase + KEY_SCHEMA_EXTENSION));
            writeSchema(topic.getValueSchema(), new File(fileBase + VALUE_SCHEMA_EXTENSION));
        }

        return ref.acquire();
    }

    private void writeSchema(Schema schema, File file) {
        try (FileOutputStream out = new FileOutputStream(file);
             OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writer.write(schema.toString(false));
        } catch (IOException ex) {
            logger.error("Cannot write schema to {}", file, ex);
        }
    }

    public synchronized <K extends SpecificRecord, V extends SpecificRecord> void releaseCache(DataCache<K, V> cache) throws IOException {
        CountedReference<DataCache> ref = caches.get(cache.getTopic().getName());
        if (ref == null) {
            throw new IllegalStateException("DataCache " + cache.getTopic() + " is not held");
        }
        DataCache storedCache = ref.release();
        if (ref.isNotHeld()) {
            storedCache.close();
            caches.remove(cache.getTopic().getName());
            if (caches.size() == 0) {
                cacheExecutorFactory.close();
                cacheExecutorFactory = null;
            }
        }
    }
}
