package com.acurast.p2p

import android.content.Context

/**
 * Gives the library the Android context it needs to read the system's DNS servers. Call it before
 * starting a node; calling it again has no effect.
 */
object AcurastP2pAndroid {
    fun init(context: Context) {
        System.loadLibrary("acurast_p2p_ffi")
        installContext(context.applicationContext)
    }

    @JvmStatic
    private external fun installContext(context: Context)
}
