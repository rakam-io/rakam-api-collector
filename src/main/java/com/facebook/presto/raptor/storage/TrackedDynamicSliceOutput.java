package com.facebook.presto.raptor.storage;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.rakam.presto.MemoryTracker;

import java.io.IOException;
import java.io.InputStream;

public class TrackedDynamicSliceOutput extends DynamicSliceOutput {
    private final MemoryTracker memoryTracker;
    private long lastRetainedSize;

    public TrackedDynamicSliceOutput(MemoryTracker memoryTracker, int estimatedSize) {
        super(estimatedSize);
        this.memoryTracker = memoryTracker;
        lastRetainedSize = getRetainedSize();
        memoryTracker.reserveMemory(lastRetainedSize);
    }

    @Override
    public void writeByte(int value) {
        super.writeByte(value);
        checkMemoryReservation();
    }

    @Override
    public void writeShort(int value) {
        super.writeShort(value);
        checkMemoryReservation();
    }

    @Override
    public void writeInt(int value) {
        super.writeInt(value);
        checkMemoryReservation();
    }

    @Override
    public void writeLong(long value) {
        super.writeLong(value);
        checkMemoryReservation();
    }

    @Override
    public void writeFloat(float value) {
        super.writeFloat(value);
        checkMemoryReservation();
    }

    @Override
    public void writeDouble(double value) {
        super.writeDouble(value);
        checkMemoryReservation();
    }

    @Override
    public void writeBytes(byte[] source) {
        super.writeBytes(source);
        checkMemoryReservation();
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length) {
        super.writeBytes(source, sourceIndex, length);
        checkMemoryReservation();
    }

    @Override
    public void writeBytes(Slice source) {
        super.writeBytes(source);
        checkMemoryReservation();
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length) {
        super.writeBytes(source, sourceIndex, length);
        checkMemoryReservation();
    }

    @Override
    public void writeBytes(InputStream in, int length) throws IOException {
        super.writeBytes(in, length);
        checkMemoryReservation();
    }

    @Override
    public void writeZero(int length) {
        super.writeZero(length);
        checkMemoryReservation();
    }

    private void checkMemoryReservation() {
        long retainedSize = getRetainedSize();
        if (retainedSize != lastRetainedSize) {
            memoryTracker.reserveMemory(retainedSize - lastRetainedSize);
            lastRetainedSize = retainedSize;
        }
    }
}
