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
package org.apache.asterix.codegen.asterix.map.entry;

import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.CONTINUE_CHUNK;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.DECODE_MASK;
import static org.apache.hyracks.util.string.UTF8StringUtil.getNumBytesToStoreLength;

import org.apache.spark.unsafe.Platform;

public class StringEntryUtil {
    private StringEntryUtil() {
    }

    public static int decode(Object srcBytes, long offset, long length) {
        int sum = 0;
        long startPos = offset;
        byte nextByte = Platform.getByte(srcBytes, offset);
        long endPos = offset + length;
        while (startPos < endPos && (nextByte & CONTINUE_CHUNK) == CONTINUE_CHUNK) {
            sum = (sum + (nextByte & DECODE_MASK)) << 7;
            startPos++;
            nextByte = Platform.getByte(srcBytes, startPos);
        }
        if (startPos < endPos) {
            sum += nextByte;
        } else {
            throw new IllegalStateException("Corrupted string bytes: trying to access entry " + startPos
                    + " in a byte array of length " + endPos);
        }
        return sum;
    }

    public static long computePrefix(Object basedObject, long start, int length) {
        int len = decode(basedObject, start, length);
        long nk = 0;
        long offset = start + getNumBytesToStoreLength(len);
        long endOfString = offset + len;
        for (int i = 0; i < 4 && endOfString > offset; ++i) {
            nk <<= 16;
            if (i < len) {
                nk += (charAt(basedObject, offset)) & 0xffff;
                offset += charSize(basedObject, offset);
            }
        }
        return nk;
    }

    public static int compare(Object leftBaseObject, long leftBaseOffset, int leftBaseLength, Object rightBaseObject,
            long rightBaseOffset, int rightBaseLength) {
        int leftLength = decode(leftBaseObject, leftBaseOffset, leftBaseLength);
        int rightLength = decode(rightBaseObject, rightBaseOffset, rightBaseLength);
        long leftActualStart = leftBaseOffset + getNumBytesToStoreLength(leftLength);
        long rightActualStart = rightBaseOffset + getNumBytesToStoreLength(rightLength);
        return compareTo(leftBaseObject, leftActualStart, leftLength, rightBaseObject, rightActualStart, rightLength);
    }

    private static int compareTo(Object leftBaseObject, long thisActualStart, int thisLength, Object rightBaseObject,
            long thatActualStart, int thatLength) {
        int c1 = 0;
        int c2 = 0;

        while (c1 < thisLength && c2 < thatLength) {
            char ch1 = charAt(leftBaseObject, thisActualStart + c1);
            char ch2 = charAt(rightBaseObject, thatActualStart + c2);

            if (ch1 != ch2) {
                return ch1 - ch2;
            }
            c1 += charSize(leftBaseObject, thisActualStart + c1);
            c2 += charSize(rightBaseObject, thatActualStart + c2);
        }
        return thisLength - thatLength;
    }

    private static char charAt(Object baseObject, long offset) {
        int c = Platform.getByte(baseObject, offset) & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return (char) c;

            case 12:
            case 13:
                return (char) (((c & 0x1F) << 6) | ((Platform.getByte(baseObject, offset + 1)) & 0x3F));

            case 14:
                return (char) (((c & 0x0F) << 12) | (((Platform.getByte(baseObject, offset + 1)) & 0x3F) << 6)
                        | (Platform.getByte(baseObject, offset + 2) & 0x3F));

            default:
                throw new IllegalArgumentException();
        }
    }

    public static int charSize(Object baseObject, long offset) {
        int c = Platform.getByte(baseObject, offset) & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return 1;
            case 12:
            case 13:
                return 2;

            case 14:
                return 3;

            default:
                throw new IllegalStateException();
        }
    }
}
