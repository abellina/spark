package org.apache.spark.util.NativeUtils;

class NativeUtils {
    private native void call();
    static {
        System.loadLibrary("native_utils");
    }
}
