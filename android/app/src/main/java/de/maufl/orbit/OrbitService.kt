package de.maufl.orbit

import android.app.Service
import android.content.Intent
import android.os.IBinder
import uniffi.pfs_uniffi.PfsClient

class OrbitService : Service() {

    lateinit var pfsClient: PfsClient

    override fun onBind(intent: Intent): IBinder {

        TODO("Return the communication channel to the service.")
    }

    override fun onCreate() {
        super.onCreate()
        pfsClient = PfsClient(filesDir.toString(), listOf<String>())
    }
}