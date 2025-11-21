package de.maufl.orbit

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.wifi.WifiManager
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.content.edit
import uniffi.orbit_android.Config
import uniffi.orbit_android.OrbitClient
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
        private const val NOTIFICATION_ID = 1
        private const val CHANNEL_ID = "orbit_service_channel"
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

        // Create notification channel and start foreground service
        createNotificationChannel()
        startForeground(NOTIFICATION_ID, createNotification())

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

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        Log.d(TAG, "onStartCommand called")
        return START_STICKY
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "Orbit Service",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Keeps Orbit filesystem running in the background"
            }

            val notificationManager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            notificationManager.createNotificationChannel(channel)
        }
    }

    private fun createNotification(): Notification {
        val notificationIntent = Intent(this, MainActivity::class.java)
        val pendingIntent = PendingIntent.getActivity(
            this,
            0,
            notificationIntent,
            PendingIntent.FLAG_IMMUTABLE
        )

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Orbit")
            .setContentText("Filesystem is running")
            .setSmallIcon(R.drawable.orbit)
            .setContentIntent(pendingIntent)
            .build()
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