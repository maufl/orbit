package de.maufl.orbit

import android.content.Context
import android.util.Log
import android.util.Size
import java.io.File
import java.security.MessageDigest

/**
 * Manages a persistent cache of thumbnails to avoid regenerating them on every request.
 * Relies on Android's automatic cache management for cleanup.
 */
class ThumbnailCache(context: Context) {
    companion object {
        private const val TAG = "ThumbnailCache"
        private const val CACHE_DIR_NAME = "thumbnails"
    }

    private val cacheDir: File = File(context.cacheDir, CACHE_DIR_NAME).apply {
        if (!exists()) {
            mkdirs()
            Log.d(TAG, "Created thumbnail cache directory: $absolutePath")
        }
    }

    /**
     * Generates a cache key from document ID and thumbnail size.
     * Uses SHA-256 hash to create a safe filename.
     */
    private fun getCacheKey(documentId: String, size: Size): String {
        val input = "$documentId-${size.width}x${size.height}"
        val bytes = MessageDigest.getInstance("SHA-256").digest(input.toByteArray())
        return bytes.joinToString("") { "%02x".format(it) } + ".png"
    }

    /**
     * Gets the cache file for a given document ID and size.
     * Returns the file if it exists, null otherwise.
     */
    fun getCachedThumbnail(documentId: String, size: Size): File? {
        val cacheKey = getCacheKey(documentId, size)
        val cacheFile = File(cacheDir, cacheKey)

        return if (cacheFile.exists() && cacheFile.isFile) {
            Log.d(TAG, "Cache hit for $documentId at ${size.width}x${size.height}")
            cacheFile
        } else {
            Log.d(TAG, "Cache miss for $documentId at ${size.width}x${size.height}")
            null
        }
    }

    /**
     * Creates a new cache file for storing a thumbnail.
     * Returns the file to write to.
     */
    fun createCacheFile(documentId: String, size: Size): File {
        val cacheKey = getCacheKey(documentId, size)
        val cacheFile = File(cacheDir, cacheKey)
        Log.d(TAG, "Creating cache file: ${cacheFile.absolutePath}")
        return cacheFile
    }
}
