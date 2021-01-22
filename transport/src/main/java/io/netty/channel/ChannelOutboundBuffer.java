/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.InternalThreadLocalMap;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.ObjectPool.Handle;
import io.netty.util.internal.ObjectPool.ObjectCreator;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PromiseNotificationUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.lang.Math.min;

/**
 * (Transport implementors only) an internal data structure used by {@link AbstractChannel} to store its pending
 * outbound write requests.
 * <p>
 * All methods must be called by a transport implementation from an I/O thread, except the following ones:
 * <ul>
 * <li>{@link #size()} and {@link #isEmpty()}</li>
 * <li>{@link #isWritable()}</li>
 * <li>{@link #getUserDefinedWritability(int)} and {@link #setUserDefinedWritability(int, boolean)}</li>
 * </ul>
 * </p>
 */
public final class ChannelOutboundBuffer {
    // Assuming a 64-bit JVM:
    //  - 16 bytes object header
    //  - 6 reference fields
    //  - 2 long fields
    //  - 2 int fields
    //  - 1 boolean field
    //  - padding
    static final int CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD =
            SystemPropertyUtil.getInt("io.netty.transport.outboundBufferEntrySizeOverhead", 96);

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelOutboundBuffer.class);

    private static final FastThreadLocal<ByteBuffer[]> NIO_BUFFERS = new FastThreadLocal<ByteBuffer[]>() {
        @Override
        protected ByteBuffer[] initialValue() throws Exception {
            return new ByteBuffer[1024];
        }
    };

    private final Channel channel;

    // Entry(flushedEntry) --> ... Entry(unflushedEntry) --> ... Entry(tailEntry)
    //
    // The Entry that is the first in the linked-list structure that was flushed
    private Entry flushedEntry;
    // The Entry which is the first unflushed in the linked-list structure
    private Entry unflushedEntry;
    // The Entry which represents the tail of the buffer
    private Entry tailEntry;
    // The number of flushed entries that are not written yet
    private int flushed;

    private int nioBufferCount;
    private long nioBufferSize;

    private boolean inFail;

    private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");

    @SuppressWarnings("UnusedDeclaration")
    private volatile long totalPendingSize;

    private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> UNWRITABLE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "unwritable");

    // 用于记录是否可写，
    // 最低位是1表示不可写，即发送缓冲区满了【也就是  unwritable=1】
    // 最低位是0表示可写【也就是 unwritable = 0】
    @SuppressWarnings("UnusedDeclaration")
    private volatile int unwritable;

    private volatile Runnable fireChannelWritabilityChangedTask;

    ChannelOutboundBuffer(AbstractChannel channel) {
        this.channel = channel;
    }

    /**
     * Add given message to this {@link ChannelOutboundBuffer}. The given {@link ChannelPromise} will be notified once
     * the message was written.
     */
    public void addMessage(Object msg, int size, ChannelPromise promise) {
        // 创建新的数据缓冲节点
        Entry entry = Entry.newInstance(msg, size, total(msg), promise);
        // 加进缓冲区
        if (tailEntry == null) {
            flushedEntry = null;
        } else {
            Entry tail = tailEntry;
            tail.next = entry;
        }
        tailEntry = entry;
        if (unflushedEntry == null) {
            unflushedEntry = entry;
        }

        // increment pending bytes after adding message to the unflushed arrays.
        // See https://github.com/netty/netty/issues/1619
        incrementPendingOutboundBytes(entry.pendingSize, false);
    }

    /**
     * Add a flush to this {@link ChannelOutboundBuffer}. This means all previous added messages are marked as flushed
     * and so you will be able to handle them.
     */
    public void addFlush() {
        // There is no need to process all entries if there was already a flush before and no new messages
        // where added in the meantime.
        //
        // See https://github.com/netty/netty/issues/2577
        Entry entry = unflushedEntry;
        if (entry != null) {
            if (flushedEntry == null) {
                // there is no flushedEntry yet, so start with the entry
                flushedEntry = entry;
            }
            do {
                flushed ++;
                if (!entry.promise.setUncancellable()) {
                    // Was cancelled so make sure we free up memory and notify about the freed bytes
                    int pending = entry.cancel();
                    decrementPendingOutboundBytes(pending, false, true);
                }
                entry = entry.next;
            } while (entry != null);

            // All flushed so reset unflushedEntry
            unflushedEntry = null;
        }
    }

    /**
     * Increment the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    void incrementPendingOutboundBytes(long size) {
        incrementPendingOutboundBytes(size, true);
    }

    private void incrementPendingOutboundBytes(long size, boolean invokeLater) {
        if (size == 0) {
            return;
        }

        long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, size);
        if (newWriteBufferSize > channel.config().getWriteBufferHighWaterMark()) {
            setUnwritable(invokeLater);
        }
    }

    /**
     * Decrement the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    void decrementPendingOutboundBytes(long size) {
        decrementPendingOutboundBytes(size, true, true);
    }

    private void decrementPendingOutboundBytes(long size, boolean invokeLater, boolean notifyWritability) {
        if (size == 0) {
            return;
        }

        // 字节长度计数减去删除的
        long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
        // 如果等待发送的字节数低于阈值，发送低水位预警，要写的可以往缓冲区写了
        if (notifyWritability && newWriteBufferSize < channel.config().getWriteBufferLowWaterMark()) {
            setWritable(invokeLater);
        }
    }

    private static long total(Object msg) {
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).count();
        }
        if (msg instanceof ByteBufHolder) {
            return ((ByteBufHolder) msg).content().readableBytes();
        }
        return -1;
    }

    /**
     * Return the current message to write or {@code null} if nothing was flushed before and so is ready to be written.
     */
    public Object current() {
        Entry entry = flushedEntry;
        if (entry == null) {
            return null;
        }

        return entry.msg;
    }

    /**
     * Return the current message flush progress.
     * @return {@code 0} if nothing was flushed before for the current message or there is no current message
     */
    public long currentProgress() {
        Entry entry = flushedEntry;
        if (entry == null) {
            return 0;
        }
        return entry.progress;
    }

    /**
     * Notify the {@link ChannelPromise} of the current message about writing progress.
     */
    public void progress(long amount) {
        Entry e = flushedEntry;
        assert e != null;
        ChannelPromise p = e.promise;
        long progress = e.progress + amount;
        e.progress = progress;
        if (p instanceof ChannelProgressivePromise) {
            ((ChannelProgressivePromise) p).tryProgress(progress, e.total);
        }
    }

    /**
     * Will remove the current message, mark its {@link ChannelPromise} as success and return {@code true}. If no
     * flushed message exists at the time this method is called it will return {@code false} to signal that no more
     * messages are ready to be handled.
     */
    public boolean remove() {
        Entry e = flushedEntry;
        if (e == null) {
            clearNioBuffers();
            return false;
        }
        Object msg = e.msg;

        ChannelPromise promise = e.promise;
        int size = e.pendingSize;

        // 移除第一个要发送数据,并更新要写入的数据
        removeEntry(e);

        if (!e.cancelled) {
            // only release message, notify and decrement if it was not canceled before.
            // 释放数据的引用
            ReferenceCountUtil.safeRelease(msg);
            safeSuccess(promise);
            // 计算剩余要发送的数据量，如果水位过低，触发低水位时间通知
            decrementPendingOutboundBytes(size, false, true);
        }

        // 移除引用，方便 GC
        // recycle the entry
        e.recycle();

        return true;
    }

    /**
     * Will remove the current message, mark its {@link ChannelPromise} as failure using the given {@link Throwable}
     * and return {@code true}. If no   flushed message exists at the time this method is called it will return
     * {@code false} to signal that no more messages are ready to be handled.
     */
    public boolean remove(Throwable cause) {
        return remove0(cause, true);
    }

    private boolean remove0(Throwable cause, boolean notifyWritability) {
        Entry e = flushedEntry;
        if (e == null) {
            clearNioBuffers();
            return false;
        }
        Object msg = e.msg;

        ChannelPromise promise = e.promise;
        int size = e.pendingSize;

        removeEntry(e);

        if (!e.cancelled) {
            // only release message, fail and decrement if it was not canceled before.
            ReferenceCountUtil.safeRelease(msg);

            safeFail(promise, cause);
            decrementPendingOutboundBytes(size, false, notifyWritability);
        }

        // recycle the entry
        e.recycle();

        return true;
    }

    private void removeEntry(Entry e) {
        if (-- flushed == 0) {// 将要写入 io 的数据块 -1
            // 如果等待发送 io 的数据，已经为空，清理一下相关的引用
            // processed everything
            flushedEntry = null;
            if (e == tailEntry) {
                tailEntry = null;
                unflushedEntry = null;
            }
        } else {
            // 更新下一个 io 要用的数据块
            flushedEntry = e.next;
        }
    }

    /**
     * Removes the fully written entries and update the reader index of the partially written entry.
     * This operation assumes all messages in this buffer is {@link ByteBuf}.
     */
    public void removeBytes(long writtenBytes) {
        for (;;) {
            Object msg = current(); // 从第一条缓冲区开始处理
            if (!(msg instanceof ByteBuf)) {
                assert writtenBytes == 0;
                break;
            }

            final ByteBuf buf = (ByteBuf) msg;
            final int readerIndex = buf.readerIndex();
            final int readableBytes = buf.writerIndex() - readerIndex;

            if (readableBytes <= writtenBytes) {// 发送的字节数大于等于可读的字节数，说明当前缓冲区是全发完的
                if (writtenBytes != 0) {
                    progress(readableBytes);// 处理一下当前发送进度，顺便该触发事件就触发事件
                    writtenBytes -= readableBytes;
                }
                remove();// 把已经发送完的缓冲区移除
            } else { // readableBytes > writtenBytes
                // 发送的字节数比当前缓冲区可读的少，说明发生了写半包，此时把当前缓冲区已经读了的字节数处理一下即可
                if (writtenBytes != 0) {
                    buf.readerIndex(readerIndex + (int) writtenBytes);
                    progress(writtenBytes);
                }
                break;
            }
        }
        // 清理掉本次生成的 ByteBuffer 数组，方便 GC ，后面继续写剩下的数据时会重新生成
        clearNioBuffers();
    }

    // Clear all ByteBuffer from the array so these can be GC'ed.
    // See https://github.com/netty/netty/issues/3837
    private void clearNioBuffers() {
        int count = nioBufferCount;
        if (count > 0) {
            nioBufferCount = 0;
            Arrays.fill(NIO_BUFFERS.get(), 0, count, null);
        }
    }

    /**
     * Returns an array of direct NIO buffers if the currently pending messages are made of {@link ByteBuf} only.
     * {@link #nioBufferCount()} and {@link #nioBufferSize()} will return the number of NIO buffers in the returned
     * array and the total number of readable bytes of the NIO buffers respectively.
     * <p>
     * Note that the returned array is reused and thus should not escape
     * {@link AbstractChannel#doWrite(ChannelOutboundBuffer)}.
     * Refer to {@link NioSocketChannel#doWrite(ChannelOutboundBuffer)} for an example.
     * </p>
     */
    public ByteBuffer[] nioBuffers() {
        return nioBuffers(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Returns an array of direct NIO buffers if the currently pending messages are made of {@link ByteBuf} only.
     * {@link #nioBufferCount()} and {@link #nioBufferSize()} will return the number of NIO buffers in the returned
     * array and the total number of readable bytes of the NIO buffers respectively.
     * <p>
     * Note that the returned array is reused and thus should not escape
     * {@link AbstractChannel#doWrite(ChannelOutboundBuffer)}.
     * Refer to {@link NioSocketChannel#doWrite(ChannelOutboundBuffer)} for an example.
     * </p>
     * @param maxCount The maximum amount of buffers that will be added to the return value.
     * @param maxBytes A hint toward the maximum number of bytes to include as part of the return value. Note that this
     *                 value maybe exceeded because we make a best effort to include at least 1 {@link ByteBuffer}
     *                 in the return value to ensure write progress is made.
     */
    public ByteBuffer[] nioBuffers(int maxCount, long maxBytes) {
        assert maxCount > 0;
        assert maxBytes > 0;
        long nioBufferSize = 0;
        int nioBufferCount = 0;
        final InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        ByteBuffer[] nioBuffers = NIO_BUFFERS.get(threadLocalMap);
        Entry entry = flushedEntry;
        while (isFlushedEntry(entry) && entry.msg instanceof ByteBuf) {// 如果是何时类型的缓冲区
            if (!entry.cancelled) {
                ByteBuf buf = (ByteBuf) entry.msg;
                final int readerIndex = buf.readerIndex();
                final int readableBytes = buf.writerIndex() - readerIndex; // 拿到可以读的字节数

                if (readableBytes > 0) {// 有可读字节数
                    if (maxBytes - readableBytes < nioBufferSize && nioBufferCount != 0) {
                        // 如果加上这次的长度，会导致要发送的总字节数大于传入的最大字节数限制，直接结束
                        // If the nioBufferSize + readableBytes will overflow maxBytes, and there is at least one entry
                        // we stop populate the ByteBuffer array. This is done for 2 reasons:
                        // 1. bsd/osx don't allow to write more bytes then Integer.MAX_VALUE with one writev(...) call
                        // and so will return 'EINVAL', which will raise an IOException. On Linux it may work depending
                        // on the architecture and kernel but to be safe we also enforce the limit here.
                        // 2. There is no sense in putting more data in the array than is likely to be accepted by the
                        // OS.
                        //
                        // See also:
                        // - https://www.freebsd.org/cgi/man.cgi?query=write&sektion=2
                        // - http://linux.die.net/man/2/writev
                        break;
                    }
                    nioBufferSize += readableBytes;// 当前统计的字节数累加
                    int count = entry.count;
                    if (count == -1) {
                        //noinspection ConstantValueVariableUse
                        entry.count = count = buf.nioBufferCount();
                    }
                    int neededSpace = min(maxCount, nioBufferCount + count);
                    if (neededSpace > nioBuffers.length) {// 该扩容就扩容
                        nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                        NIO_BUFFERS.set(threadLocalMap, nioBuffers);
                    }
                    if (count == 1) {
                        ByteBuffer nioBuf = entry.buf;
                        if (nioBuf == null) {
                            // cache ByteBuffer as it may need to create a new ByteBuffer instance if its a
                            // derived buffer
                            entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
                        }
                        nioBuffers[nioBufferCount++] = nioBuf;
                    } else {
                        // The code exists in an extra method to ensure the method is not too big to inline as this
                        // branch is not very likely to get hit very frequently.
                        nioBufferCount = nioBuffers(entry, buf, nioBuffers, nioBufferCount, maxCount);
                    }
                    if (nioBufferCount >= maxCount) {
                        break;
                    }
                }
            }
            entry = entry.next;
        }
        this.nioBufferCount = nioBufferCount;
        this.nioBufferSize = nioBufferSize;

        return nioBuffers;
    }

    private static int nioBuffers(Entry entry, ByteBuf buf, ByteBuffer[] nioBuffers, int nioBufferCount, int maxCount) {
        ByteBuffer[] nioBufs = entry.bufs;
        if (nioBufs == null) {
            // cached ByteBuffers as they may be expensive to create in terms
            // of Object allocation
            entry.bufs = nioBufs = buf.nioBuffers();
        }
        for (int i = 0; i < nioBufs.length && nioBufferCount < maxCount; ++i) {
            ByteBuffer nioBuf = nioBufs[i];
            if (nioBuf == null) {
                break;
            } else if (!nioBuf.hasRemaining()) {
                continue;
            }
            nioBuffers[nioBufferCount++] = nioBuf;
        }
        return nioBufferCount;
    }

    private static ByteBuffer[] expandNioBufferArray(ByteBuffer[] array, int neededSpace, int size) {
        int newCapacity = array.length;
        do {
            // double capacity until it is big enough
            // See https://github.com/netty/netty/issues/1890
            newCapacity <<= 1;

            if (newCapacity < 0) {
                throw new IllegalStateException();
            }

        } while (neededSpace > newCapacity);

        ByteBuffer[] newArray = new ByteBuffer[newCapacity];
        System.arraycopy(array, 0, newArray, 0, size);

        return newArray;
    }

    /**
     * Returns the number of {@link ByteBuffer} that can be written out of the {@link ByteBuffer} array that was
     * obtained via {@link #nioBuffers()}. This method <strong>MUST</strong> be called after {@link #nioBuffers()}
     * was called.
     */
    public int nioBufferCount() {
        return nioBufferCount;
    }

    /**
     * Returns the number of bytes that can be written out of the {@link ByteBuffer} array that was
     * obtained via {@link #nioBuffers()}. This method <strong>MUST</strong> be called after {@link #nioBuffers()}
     * was called.
     */
    public long nioBufferSize() {
        return nioBufferSize;
    }

    /**
     * Returns {@code true} if and only if {@linkplain #totalPendingWriteBytes() the total number of pending bytes} did
     * not exceed the write watermark of the {@link Channel} and
     * no {@linkplain #setUserDefinedWritability(int, boolean) user-defined writability flag} has been set to
     * {@code false}.
     */
    public boolean isWritable() {
        return unwritable == 0;
    }

    /**
     * Returns {@code true} if and only if the user-defined writability flag at the specified index is set to
     * {@code true}.
     */
    public boolean getUserDefinedWritability(int index) {
        return (unwritable & writabilityMask(index)) == 0;
    }

    /**
     * Sets a user-defined writability flag at the specified index.
     */
    public void setUserDefinedWritability(int index, boolean writable) {
        if (writable) {
            setUserDefinedWritability(index);
        } else {
            clearUserDefinedWritability(index);
        }
    }

    private void setUserDefinedWritability(int index) {
        final int mask = ~writabilityMask(index);
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue & mask;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                if (oldValue != 0 && newValue == 0) {
                    fireChannelWritabilityChanged(true);
                }
                break;
            }
        }
    }

    private void clearUserDefinedWritability(int index) {
        final int mask = writabilityMask(index);
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue | mask;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                if (oldValue == 0 && newValue != 0) {
                    fireChannelWritabilityChanged(true);
                }
                break;
            }
        }
    }

    private static int writabilityMask(int index) {
        if (index < 1 || index > 31) {
            throw new IllegalArgumentException("index: " + index + " (expected: 1~31)");
        }
        return 1 << index;
    }

    /**
     * 发送低水位事件，也就是触发可写的事件
     *
     * @param invokeLater
     */
    private void setWritable(boolean invokeLater) {
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue & ~1;// 保证最低位是0
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {//设置成功
                if (oldValue != 0 && newValue == 0) {// 设置成功，而且之前最低位是1，也就是发生了标记量的变化
                    // 需要触发事件
                    fireChannelWritabilityChanged(invokeLater);
                }
                break;
            }
        }
    }

    private void setUnwritable(boolean invokeLater) {
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue | 1;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                if (oldValue == 0 && newValue != 0) {
                    fireChannelWritabilityChanged(invokeLater);
                }
                break;
            }
        }
    }

    private void fireChannelWritabilityChanged(boolean invokeLater) {
        // 拿到 pipeLine
        //
        final ChannelPipeline pipeline = channel.pipeline();
        if (invokeLater) {// 如果是异步通知，就通过提交任务的形式解耦
            Runnable task = fireChannelWritabilityChangedTask;
            if (task == null) {
                fireChannelWritabilityChangedTask = task = new Runnable() {
                    @Override
                    public void run() {
                        pipeline.fireChannelWritabilityChanged();
                    }
                };
            }
            channel.eventLoop().execute(task);
        } else {
            pipeline.fireChannelWritabilityChanged();
        }
    }

    /**
     * Returns the number of flushed messages in this {@link ChannelOutboundBuffer}.
     */
    public int size() {
        return flushed;
    }

    /**
     * Returns {@code true} if there are flushed messages in this {@link ChannelOutboundBuffer} or {@code false}
     * otherwise.
     */
    public boolean isEmpty() {
        return flushed == 0;
    }

    void failFlushed(Throwable cause, boolean notify) {
        // Make sure that this method does not reenter.  A listener added to the current promise can be notified by the
        // current thread in the tryFailure() call of the loop below, and the listener can trigger another fail() call
        // indirectly (usually by closing the channel.)
        //
        // See https://github.com/netty/netty/issues/1501
        if (inFail) {
            return;
        }

        try {
            inFail = true;
            for (;;) {
                if (!remove0(cause, notify)) {
                    break;
                }
            }
        } finally {
            inFail = false;
        }
    }

    void close(final Throwable cause, final boolean allowChannelOpen) {
        if (inFail) {
            channel.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    close(cause, allowChannelOpen);
                }
            });
            return;
        }

        inFail = true;

        if (!allowChannelOpen && channel.isOpen()) {
            throw new IllegalStateException("close() must be invoked after the channel is closed.");
        }

        if (!isEmpty()) {
            throw new IllegalStateException("close() must be invoked after all flushed writes are handled.");
        }

        // Release all unflushed messages.
        try {
            Entry e = unflushedEntry;
            while (e != null) {
                // Just decrease; do not trigger any events via decrementPendingOutboundBytes()
                int size = e.pendingSize;
                TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);

                if (!e.cancelled) {
                    // 释放引用方便 GC
                    ReferenceCountUtil.safeRelease(e.msg);
                    safeFail(e.promise, cause);
                }
                e = e.recycleAndGetNext();
            }
        } finally {
            inFail = false;
        }
        clearNioBuffers();
    }

    void close(ClosedChannelException cause) {
        close(cause, false);
    }

    private static void safeSuccess(ChannelPromise promise) {
        // Only log if the given promise is not of type VoidChannelPromise as trySuccess(...) is expected to return
        // false.
        PromiseNotificationUtil.trySuccess(promise, null, promise instanceof VoidChannelPromise ? null : logger);
    }

    private static void safeFail(ChannelPromise promise, Throwable cause) {
        // Only log if the given promise is not of type VoidChannelPromise as tryFailure(...) is expected to return
        // false.
        PromiseNotificationUtil.tryFailure(promise, cause, promise instanceof VoidChannelPromise ? null : logger);
    }

    @Deprecated
    public void recycle() {
        // NOOP
    }

    public long totalPendingWriteBytes() {
        return totalPendingSize;
    }

    /**
     * Get how many bytes can be written until {@link #isWritable()} returns {@code false}.
     * This quantity will always be non-negative. If {@link #isWritable()} is {@code false} then 0.
     */
    public long bytesBeforeUnwritable() {
        long bytes = channel.config().getWriteBufferHighWaterMark() - totalPendingSize;
        // If bytes is negative we know we are not writable, but if bytes is non-negative we have to check writability.
        // Note that totalPendingSize and isWritable() use different volatile variables that are not synchronized
        // together. totalPendingSize will be updated before isWritable().
        if (bytes > 0) {
            return isWritable() ? bytes : 0;
        }
        return 0;
    }

    /**
     * Get how many bytes must be drained from the underlying buffer until {@link #isWritable()} returns {@code true}.
     * This quantity will always be non-negative. If {@link #isWritable()} is {@code true} then 0.
     */
    public long bytesBeforeWritable() {
        long bytes = totalPendingSize - channel.config().getWriteBufferLowWaterMark();
        // If bytes is negative we know we are writable, but if bytes is non-negative we have to check writability.
        // Note that totalPendingSize and isWritable() use different volatile variables that are not synchronized
        // together. totalPendingSize will be updated before isWritable().
        if (bytes > 0) {
            return isWritable() ? 0 : bytes;
        }
        return 0;
    }

    /**
     * Call {@link MessageProcessor#processMessage(Object)} for each flushed message
     * in this {@link ChannelOutboundBuffer} until {@link MessageProcessor#processMessage(Object)}
     * returns {@code false} or there are no more flushed messages to process.
     */
    public void forEachFlushedMessage(MessageProcessor processor) throws Exception {
        ObjectUtil.checkNotNull(processor, "processor");

        Entry entry = flushedEntry;
        if (entry == null) {
            return;
        }

        do {
            if (!entry.cancelled) {
                if (!processor.processMessage(entry.msg)) {
                    return;
                }
            }
            entry = entry.next;
        } while (isFlushedEntry(entry));
    }

    private boolean isFlushedEntry(Entry e) {
        return e != null && e != unflushedEntry;
    }

    public interface MessageProcessor {
        /**
         * Will be called for each flushed message until it either there are no more flushed messages or this
         * method returns {@code false}.
         */
        boolean processMessage(Object msg) throws Exception;
    }

    static final class Entry {
        private static final ObjectPool<Entry> RECYCLER = ObjectPool.newPool(new ObjectCreator<Entry>() {
            @Override
            public Entry newObject(Handle<Entry> handle) {
                return new Entry(handle);
            }
        });

        private final Handle<Entry> handle;
        Entry next;
        Object msg;
        ByteBuffer[] bufs;
        ByteBuffer buf;
        ChannelPromise promise;
        long progress;
        long total;
        int pendingSize;
        int count = -1;
        boolean cancelled;

        private Entry(Handle<Entry> handle) {
            this.handle = handle;
        }

        static Entry newInstance(Object msg, int size, long total, ChannelPromise promise) {
            Entry entry = RECYCLER.get();
            entry.msg = msg;
            entry.pendingSize = size + CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD;
            entry.total = total;
            entry.promise = promise;
            return entry;
        }

        int cancel() {
            if (!cancelled) {
                cancelled = true;
                int pSize = pendingSize;

                // release message and replace with an empty buffer
                ReferenceCountUtil.safeRelease(msg);
                msg = Unpooled.EMPTY_BUFFER;

                pendingSize = 0;
                total = 0;
                progress = 0;
                bufs = null;
                buf = null;
                return pSize;
            }
            return 0;
        }

        void recycle() {
            next = null;
            bufs = null;
            buf = null;
            msg = null;
            promise = null;
            progress = 0;
            total = 0;
            pendingSize = 0;
            count = -1;
            cancelled = false;
            handle.recycle(this);
        }

        Entry recycleAndGetNext() {
            Entry next = this.next;
            recycle();
            return next;
        }
    }
}
