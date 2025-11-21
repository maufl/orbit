package de.maufl.orbit

import android.content.Context
import android.util.Log
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import java.util.concurrent.TimeUnit

object OrbitServiceScheduler {
    private const val TAG = "OrbitServiceScheduler"
    private const val WORK_NAME = "orbit_service_keepalive"

    fun schedulePeriodicWork(context: Context) {
        Log.d(TAG, "Scheduling periodic work to keep OrbitService running")

        val workRequest = PeriodicWorkRequestBuilder<OrbitServiceWorker>(
            15, // Repeat every 15 minutes (minimum allowed by Android)
            TimeUnit.MINUTES
        ).build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            WORK_NAME,
            ExistingPeriodicWorkPolicy.KEEP, // Don't restart if already scheduled
            workRequest
        )

        Log.d(TAG, "Periodic work scheduled")
    }
}
