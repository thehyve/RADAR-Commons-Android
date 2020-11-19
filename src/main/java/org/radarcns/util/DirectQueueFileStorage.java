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

package org.radarcns.util;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A storage backend for a QueueFile
 * @author Joris Borgdorff (joris@thehyve.nl)
 */
public class DirectQueueFileStorage implements QueueStorage {
    /** Initial file size in bytes. */
    public static final int MINIMUM_LENGTH = 4096; // one file system block

    /**
     * The underlying file. Uses a ring buffer to store entries.
     * <pre>
     * Format:
     *   QueueFileHeader.HEADER_LENGTH bytes    Header
     *   length bytes                           Data
     * </pre>
     */
    private final FileChannel channel;
    private final RandomAccessFile randomAccessFile;

    /** Filename, for toString purposes */
    private final String fileName;
    private int maximumLength;

    private boolean closed;
    private int length;
    private final boolean existed;

    /**
     * Create a new QueueFileStorage from file.
     * @param file file to use
     * @param initialLength initial length if the file does not exist.
     * @param maximumLength maximum length that the file may have.
     * @throws NullPointerException if file is null
     * @throws IllegalArgumentException if the initialLength or maximumLength is smaller than
     *                                  {@code QueueFileHeader.HEADER_LENGTH}.
     * @throws IOException if the file could not be accessed or was smaller than
     *                     {@code QueueFileHeader.HEADER_LENGTH}
     */
    public DirectQueueFileStorage(File file, int initialLength, int maximumLength) throws IOException {
        this.fileName = file.getName();
        if (initialLength < MINIMUM_LENGTH) {
            throw new IllegalArgumentException("Initial length " + initialLength
                    + " is smaller than minimum length " + getMinimumLength());
        }
        if (maximumLength < MINIMUM_LENGTH) {
            throw new IllegalArgumentException("Maximum length " + maximumLength
                    + " is smaller than minimum length " + getMinimumLength());
        }

        closed = false;
        this.maximumLength = maximumLength;

        existed = file.exists();
        randomAccessFile = new RandomAccessFile(file, "rw");

        if (existed) {
            // Read header from file
            long currentLength = randomAccessFile.length();
            if (currentLength < QueueFileHeader.HEADER_LENGTH) {
                throw new IOException("File length " + length + " is smaller than queue header length " + QueueFileHeader.HEADER_LENGTH);
            }
            length = (int)currentLength;
        } else {
            randomAccessFile.setLength(initialLength);
            length = initialLength;
        }
        channel = randomAccessFile.getChannel();
    }

    @Override
    public long read(long position, byte[] buffer, int offset, int count) throws IOException {
        requireNotClosed();
        checkOffsetAndCount(buffer, offset, count);
        int wrappedPosition = wrapPosition(position);
        ByteBuffer dst = ByteBuffer.wrap(buffer, offset, count);
        channel.position(wrappedPosition);
        if (position + count <= length) {
            readFully(dst, count);
            return wrapPosition(wrappedPosition + count);
        } else {
            // The read overlaps the EOF.
            // # of bytes to read before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
            int firstPart = length - wrappedPosition;
            readFully(dst, firstPart);
            channel.position(QueueFileHeader.HEADER_LENGTH);
            readFully(dst, count - firstPart);
            return QueueFileHeader.HEADER_LENGTH + count - firstPart;
        }
    }

    private void readFully(ByteBuffer buffer, int count) throws IOException {
        int n = 0;
        while (n < count) {
            long numRead = channel.read(buffer);
            if (numRead == -1) {
                throw new EOFException();
            }
            n += numRead;
        }
    }

    /** Wraps the position if it exceeds the end of the file. */
    private int wrapPosition(long position) {
        long newPosition = position < length ? position : QueueFileHeader.HEADER_LENGTH + position - length;
        if (newPosition >= length || position < 0) {
            throw new IllegalArgumentException("Position " + position + " invalid outside of storage length " + length);
        }
        return (int)newPosition;
    }

    /** Sets the length of the file. */
    @Override
    public void resize(long newLength) throws IOException {
        requireNotClosed();
        if (newLength > maximumLength) {
            throw new IllegalArgumentException("New length " + newLength
                    + " exceeds maximum length " + maximumLength);
        }
        if (newLength < MINIMUM_LENGTH) {
            throw new IllegalArgumentException("New length " + newLength
                    + " is less than minimum length " + QueueFileHeader.HEADER_LENGTH);
        }
        flush();
        randomAccessFile.setLength(newLength);
        channel.force(true);
        length = (int)newLength;
    }

    @Override
    public void flush() throws IOException {
        channel.force(false);
    }

    @Override
    public long write(long position, byte[] buffer, int offset, int count) throws IOException {
        requireNotClosed();
        checkOffsetAndCount(buffer, offset, count);
        int wrappedPosition = wrapPosition(position);

        ByteBuffer dst = ByteBuffer.wrap(buffer, offset, count);
        channel.position(wrappedPosition);
        int linearPart = length - wrappedPosition;
        if (linearPart >= count) {
            writeFully(dst, count);
            return wrapPosition(wrappedPosition + count);
        } else {
            // The write overlaps the EOF.
            // # of bytes to write before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
            if (linearPart > 0) {
                writeFully(dst, linearPart);
            }
            channel.position(QueueFileHeader.HEADER_LENGTH);
            writeFully(dst, count - linearPart);
            return QueueFileHeader.HEADER_LENGTH + count - linearPart;
        }
    }

    private void writeFully(ByteBuffer buffer, int count) throws IOException {
        int n = 0;
        ByteBuffer writeBuffer;
        if (buffer.remaining() == count) {
            writeBuffer = buffer;
        } else if (buffer.remaining() > count) {
            writeBuffer = buffer.slice();
            writeBuffer.limit(count);
        } else {
            throw new BufferUnderflowException();
        }

        while (n < count) {
            int numWritten = channel.write(writeBuffer);
            if (numWritten == -1) {
                throw new EOFException();
            }
            n += numWritten;
        }

        if (writeBuffer != buffer) {
            buffer.position(buffer.position() + count);
        }
    }

    @Override
    public void move(long srcPosition, long dstPosition, long count) throws IOException {
        requireNotClosed();
        if (srcPosition < 0 || dstPosition < 0 || count <= 0
                || srcPosition + count > length || dstPosition + count > length) {
            throw new IllegalArgumentException("Movement specification src=" + srcPosition
                    + ", count=" + count + ", dst=" + dstPosition
                    + " is invalid for storage of length " + length);
        }
        flush();
        channel.position(dstPosition);

        if (channel.transferTo(srcPosition, count, channel) != count) {
            throw new IOException("Cannot move all data");
        }
    }

    private void requireNotClosed() throws IOException {
        if (closed) {
            throw new QueueClosedIOException();
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        channel.close();
        randomAccessFile.close();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "<" + fileName + ">[length=" + length + "]";
    }

    private void checkOffsetAndCount(byte[] bytes, int offset, int count) {
        if (offset < 0) {
            throw new IndexOutOfBoundsException("offset < 0");
        }
        if (count < 0) {
            throw new IndexOutOfBoundsException("count < 0");
        }
        if (count + QueueFileHeader.HEADER_LENGTH > length) {
            throw new IllegalArgumentException("buffer count " + count
                    + " exceeds storage length " + length);
        }
        if (offset + count > bytes.length) {
            throw new IndexOutOfBoundsException(
                    "extent of offset and length larger than buffer length");
        }
    }

    /** File size in bytes. */
    @Override
    public long length() {
        return length;
    }

    @Override
    public long getMinimumLength() {
        return MINIMUM_LENGTH;
    }

    @Override
    public long getMaximumLength() {
        return maximumLength;
    }

    @Override
    public void setMaximumLength(long newLength) {
        if (newLength < MINIMUM_LENGTH || newLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Maximum cache size out of range "
                    + MINIMUM_LENGTH + " <= " + newLength + " <= " + Integer.MAX_VALUE);
        }
        this.maximumLength = (int)newLength;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean existed() {
        return existed;
    }
}
