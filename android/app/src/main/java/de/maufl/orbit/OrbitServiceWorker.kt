package de.maufl.orbit

import android.content.Context
import android.content.Intent
import android.util.Log
import androidx.work.Worker
import androidx.work.WorkerParameters

class OrbitServiceWorker(
    context: Context,
    workerParams: WorkerParameters
) : Worker(context, workerParams) {

    companion object {
        private const val TAG = "OrbitServiceWorker"
    }

    override fun doWork(): Result {
        Log.d(TAG, "Periodic work triggered, ensuring OrbitService is running")

        val intent = Intent(applicationContext, OrbitService::class.java)
        applicationContext.startService(intent)

        return Result.success()
    }
}
