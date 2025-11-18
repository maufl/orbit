package de.maufl.orbit

import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.wifi.WifiManager
import android.os.Binder
import android.os.IBinder
import android.util.Log
import uniffi.orbit_android.Config
import uniffi.orbit_android.OrbitClient
import androidx.core.content.edit
import uniffi.orbit_android.DirectoryEntryInfo
import uniffi.orbit_android.FsNodeInfo
import uniffi.orbit_android.FileRequestCallback
import uniffi.orbit_android.FileRequestResult
import uniffi.orbit_android.RootChangeCallback

class OrbitService : Service() {

    lateinit private var orbitClient: OrbitClient
    private val binder = OrbitBinder()
    private var multicastLock: WifiManager.MulticastLock? = null

    companion object {
        private const val TAG = "OrbitService"
        private const val PREFS_NAME = "orbit_prefs"
        private const val KEY_PRIVATE_KEY = "private_key"
    }

    inner class OrbitBinder : Binder() {
        fun getService(): OrbitService = this@OrbitService
    }

    override fun onBind(intent: Intent): IBinder {
        Log.d(TAG, "onBind called")
        return binder
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

        val wifiManager = getSystemService(Context.WIFI_SERVICE) as WifiManager
        multicastLock = wifiManager.createMulticastLock("iroh_mdns")
        multicastLock?.acquire()

        // Initialize Orbit client
        orbitClient = OrbitClient(config)

        // If we didn't have a private key, save the newly generated one
        if (privateKey == null) {
            val newConfig = orbitClient.getConfig()
            newConfig.privateKey?.let { newKey ->
                prefs.edit { putString(KEY_PRIVATE_KEY, newKey) }
                Log.i(TAG, "Saved new secret key to SharedPreferences")
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        multicastLock?.release()
        orbitClient.destroy()   
    }

    fun getFsNodeByPath(path: String) : FsNodeInfo {
        return orbitClient.getNodeByPath(path)
    }

    fun getDirectoryEntries(path: String) : List<DirectoryEntryInfo> {
        return orbitClient.listDirectory(path)
    }

    fun createFile(parentPath: String, filename: String) {
        orbitClient.createFile(parentPath, filename)
    }

    fun getBackingFilePath(path: String) : String {
        return orbitClient.getBackingFilePath(path)
    }

    fun updateFileFrom(path: String, sourcePath: String) {
        orbitClient.updateFileFrom(path, sourcePath)
    }

    fun requestFile(path: String, timeoutSeconds: ULong, callback: FileRequestCallback) {
        orbitClient.requestFile(path, timeoutSeconds, callback)
    }

    fun registerRootChangeCallback(callback: RootChangeCallback) {
        orbitClient.registerRootChangeCallback(callback)
    }
}