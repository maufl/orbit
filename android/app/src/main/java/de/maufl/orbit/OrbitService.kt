package de.maufl.orbit

import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.IBinder
import android.util.Log
import uniffi.pfs_uniffi.Config
import uniffi.pfs_uniffi.PfsClient
import androidx.core.content.edit

class OrbitService : Service() {

    lateinit var pfsClient: PfsClient

    companion object {
        private const val TAG = "OrbitService"
        private const val PREFS_NAME = "pfs_prefs"
        private const val KEY_PRIVATE_KEY = "private_key"
    }

    override fun onBind(intent: Intent): IBinder {
        TODO("Return the communication channel to the service.")
    }

    override fun onCreate() {
        super.onCreate()

        // Get SharedPreferences
        val prefs = getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

        // Try to read existing secret key
        val privateKey = prefs.getString(KEY_PRIVATE_KEY, null)

        if (privateKey != null) {
            Log.i(TAG, "Found existing secret key in SharedPreferences")
        } else {
            Log.i(TAG, "No secret key found, will generate a new one")
        }

        // Create config with optional private key
        val config = Config(
            privateKey = privateKey,
            dataDir = filesDir.toString(),
            peerNodeIds = listOf()
        )

        // Initialize PFS client
        pfsClient = PfsClient(config)

        // If we didn't have a private key, save the newly generated one
        if (privateKey == null) {
            val newConfig = pfsClient.getConfig()
            newConfig.privateKey?.let { newKey ->
                prefs.edit { putString(KEY_PRIVATE_KEY, newKey) }
                Log.i(TAG, "Saved new secret key to SharedPreferences")
            }
        }
    }
}