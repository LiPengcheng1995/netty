/*
 * Copyright 2012 The Netty Project
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

import java.util.ArrayList;
import java.util.List;

import static io.netty.util.internal.ObjectUtil.checkPositive;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * The {@link RecvByteBufAllocator} that automatically increases and
 * decreases the predicted buffer size on feed back.
 * <p>
 * It gradually increases the expected number of readable bytes if the previous
 * read fully filled the allocated buffer.  It gradually decreases the expected
 * number of readable bytes if the read operation was not able to fill a certain
 * amount of the allocated buffer two times consecutively.  Otherwise, it keeps
 * returning the same prediction.
 */
// 这个实现相比 FixedRecvByteBufAllocator ，多了一个功能：针对 IO 的反馈进行缓冲区大小预测的增减
//
// 功能实现策略如下： 如果之前从 IO 线程的读操作填满了缓冲区，就慢慢增加缓冲区预测的大小。如果之前从 IO 线程的读操作连续两次
// 没有用完缓冲区，这就慢慢降低预测。如果都没满足条件，说明预测的比较合理，就不用动了

// 此实现优点如下：
// 1. 作为一个通用的 NIO 框架，对不同的应用场景进行假设，能最大程度的满足不同的业务场景
// 2. 性能更高，尽量避免频繁申请空间的
// 3. 减少了空间的浪费
public class AdaptiveRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {

    // 最小缓冲区长度为 64 字节
    static final int DEFAULT_MINIMUM = 64;
    // 出事容量为 2048 字节
    // Use an initial value that is bigger than the common MTU of 1500
    static final int DEFAULT_INITIAL = 2048;
    // 最大缓冲区长度
    static final int DEFAULT_MAXIMUM = 65536;

    // 注意，这里的步长不是指字节数，而是 SIZE_TABLE 的下标移动的步长
    // 扩张缓冲区的步长
    private static final int INDEX_INCREMENT = 4;
    // 收缩缓冲区的步长
    private static final int INDEX_DECREMENT = 1;

    private static final int[] SIZE_TABLE;

    static {
        // 这个是为了方便计算长度定义的长度向量表
        // 数组的每个值代表一个 buffer 容量
        // 当容量小于 512 字节的时候，因为缓冲区已经比较小，可以慢慢调整，每次增加16字节
        // 当容量大于 512 字节的时候，缓冲区已经扩张到比较大了，说明要处理的是比较大的信息流，直接指数扩张
        //
        // TODO 其实这个跟我们之前介绍的扩张收缩思路不太一样，之前是比较小的时候进行高倍率扩张，比较大的时候进行定长扩张
        //
        // TODO 这里有最大最小的限制（而且是比较理性的限制，不是 Integer.MAX_VALUE 那种），所以不管怎么扩张，至少不会爆炸（计算机不会爆炸，能分配
        // TODO 出来，同时业务逻辑的执行不会爆炸，不会因为分配太大导致某些操作卡死，比如都在读东西，IO线程停住了之类的）
        // TODO 在上面这两个前提下，感觉扩张策略就可以根据具体情况进行定制了，具体问题具体分析吧，感觉能自圆其说就行
        List<Integer> sizeTable = new ArrayList<Integer>();
        for (int i = 16; i < 512; i += 16) {
            sizeTable.add(i);
        }

        for (int i = 512; i > 0; i <<= 1) {
            sizeTable.add(i);
        }

        SIZE_TABLE = new int[sizeTable.size()];
        for (int i = 0; i < SIZE_TABLE.length; i ++) {
            SIZE_TABLE[i] = sizeTable.get(i);
        }
    }

    /**
     * @deprecated There is state for {@link #maxMessagesPerRead()} which is typically based upon channel type.
     */
    @Deprecated
    public static final AdaptiveRecvByteBufAllocator DEFAULT = new AdaptiveRecvByteBufAllocator();

    /**
     * 根据传入的容量查询向量表对应的索引
     * 二分查找
     * @param size
     * @return
     */
    private static int getSizeTableIndex(final int size) {
        for (int low = 0, high = SIZE_TABLE.length - 1;;) {
            if (high < low) {
                return low;
            }
            if (high == low) {
                return high;
            }

            int mid = low + high >>> 1;
            int a = SIZE_TABLE[mid];
            int b = SIZE_TABLE[mid + 1];
            if (size > b) {
                low = mid + 1;
            } else if (size < a) {
                high = mid - 1;
            } else if (size == a) {
                return mid;
            } else {//size == b
                return mid + 1;
            }
        }
    }

    private final class HandleImpl extends MaxMessageHandle {
        // 对应向量表的最小、最大、当前索引
        private final int minIndex;
        private final int maxIndex;
        private int index;
        // 下一次预分配的 Buffer 大小
        private int nextReceiveBufferSize;
        // 是否立即执行容量所容操作
        private boolean decreaseNow;

        HandleImpl(int minIndex, int maxIndex, int initial) {
            this.minIndex = minIndex;
            this.maxIndex = maxIndex;

            index = getSizeTableIndex(initial);
            nextReceiveBufferSize = SIZE_TABLE[index];
        }

        @Override
        public void lastBytesRead(int bytes) {
            // If we read as much as we asked for we should check if we need to ramp up the size of our next guess.
            // This helps adjust more quickly when large amounts of data is pending and can avoid going back to
            // the selector to check for more data. Going back to the selector can add significant latency for large
            // data transfers.
            // 这里和预计读取的数据量比较，避免了从多路复用器检查时的开销
            // 预计读取的数据量，其实就是从 ByteBuf 拿到的 writableBytes()，也就是分配的缓冲区大小。
            if (bytes == attemptedBytesRead()) {// 缓冲区用完了，直接估计下一次申请缓冲区的大小
                record(bytes);
            }
            // 修改计数
            super.lastBytesRead(bytes);
        }

        @Override
        public int guess() {
            return nextReceiveBufferSize;
        }

        /**
         * NioSocketChannel 执行完读方法后，会调用到这里进行记录本轮轮询得到的总字节数
         *
         * 执行此方法会根据实际读取的字节数对 ByteBuf 进行动态的收缩和扩张
         * @param actualReadBytes
         */
        private void record(int actualReadBytes) {
            // 首先拿到收缩后的 ByteBuf 长度。
            if (actualReadBytes <= SIZE_TABLE[max(0, index - INDEX_DECREMENT)]) {//如果读取的字节数比比收缩后的容量还少，就处理收缩
                if (decreaseNow) {// decreaseNow 为 true ，表示之前已经出现过一次缓冲区没充分利用，这里可以直接收缩
                    index = max(index - INDEX_DECREMENT, minIndex);
                    nextReceiveBufferSize = SIZE_TABLE[index];
                    decreaseNow = false;
                } else {// 这是第一次缓冲区没充分利用，做一下标记即可，避免频繁改动缓冲读长度
                    decreaseNow = true;
                }
            } else if (actualReadBytes >= nextReceiveBufferSize) {// 如果读取的字节数比下次要扩张的缓冲区容量还大，就做一次扩张
                index = min(index + INDEX_INCREMENT, maxIndex);
                nextReceiveBufferSize = SIZE_TABLE[index];
                decreaseNow = false;// 扩张后重置收缩标记量
            }
        }

        @Override
        public void readComplete() {
            record(totalBytesRead());
        }
    }

    private final int minIndex;
    private final int maxIndex;
    private final int initial;

    /**
     * Creates a new predictor with the default parameters.  With the default
     * parameters, the expected buffer size starts from {@code 1024}, does not
     * go down below {@code 64}, and does not go up above {@code 65536}.
     */
    public AdaptiveRecvByteBufAllocator() {
        this(DEFAULT_MINIMUM, DEFAULT_INITIAL, DEFAULT_MAXIMUM);
    }

    /**
     * Creates a new predictor with the specified parameters.
     *
     * @param minimum  the inclusive lower bound of the expected buffer size
     * @param initial  the initial buffer size when no feed back was received
     * @param maximum  the inclusive upper bound of the expected buffer size
     */
    public AdaptiveRecvByteBufAllocator(int minimum, int initial, int maximum) {
        checkPositive(minimum, "minimum");
        if (initial < minimum) {
            throw new IllegalArgumentException("initial: " + initial);
        }
        if (maximum < initial) {
            throw new IllegalArgumentException("maximum: " + maximum);
        }

        int minIndex = getSizeTableIndex(minimum);
        if (SIZE_TABLE[minIndex] < minimum) {
            this.minIndex = minIndex + 1;
        } else {
            this.minIndex = minIndex;
        }

        int maxIndex = getSizeTableIndex(maximum);
        if (SIZE_TABLE[maxIndex] > maximum) {
            this.maxIndex = maxIndex - 1;
        } else {
            this.maxIndex = maxIndex;
        }

        this.initial = initial;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Handle newHandle() {
        return new HandleImpl(minIndex, maxIndex, initial);
    }

    @Override
    public AdaptiveRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
        super.respectMaybeMoreData(respectMaybeMoreData);
        return this;
    }
}
