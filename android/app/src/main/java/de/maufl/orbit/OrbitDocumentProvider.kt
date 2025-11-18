package de.maufl.orbit

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.database.Cursor
import android.database.MatrixCursor
import android.graphics.Point
import android.os.Bundle
import android.os.CancellationSignal
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.provider.DocumentsContract
import android.provider.DocumentsProvider
import android.system.OsConstants
import android.util.Log
import android.webkit.MimeTypeMap
import uniffi.orbit_android.FileKind
import uniffi.orbit_android.FileRequestCallback
import uniffi.orbit_android.FileRequestResult
import java.io.FileNotFoundException

class OrbitDocumentProvider : DocumentsProvider() {

    companion object {
        private const val TAG = "OrbitDocumentProvider"
        private const val AUTHORITY = "de.maufl.orbit.documents"

        private val DEFAULT_ROOT_PROJECTION = arrayOf(
            DocumentsContract.Root.COLUMN_ROOT_ID,
            DocumentsContract.Root.COLUMN_FLAGS,
            DocumentsContract.Root.COLUMN_ICON,
            DocumentsContract.Root.COLUMN_TITLE,
            DocumentsContract.Root.COLUMN_DOCUMENT_ID,
        )

        private val DEFAULT_DOCUMENT_PROJECTION = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_MIME_TYPE,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME,
            DocumentsContract.Document.COLUMN_LAST_MODIFIED,
            DocumentsContract.Document.COLUMN_FLAGS,
            DocumentsContract.Document.COLUMN_SIZE
        )
    }

    private var orbitService: OrbitService.OrbitBinder? = null
    private val serviceConnection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
            Log.d(TAG, "Service connected")
            orbitService = service as? OrbitService.OrbitBinder
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            Log.d(TAG, "Service disconnected")
            orbitService = null
        }
    }

    override fun onCreate(): Boolean {
        Log.d(TAG, "onCreate")
        val intent = Intent(context, OrbitService::class.java)
        context?.bindService(intent, serviceConnection, Context.BIND_AUTO_CREATE)
        return true
    }

    override fun queryRoots(projection: Array<out String>?): Cursor {
        Log.d(TAG, "queryRoots")
        val result = MatrixCursor(projection ?: DEFAULT_ROOT_PROJECTION)

        val rootNode = orbitService?.getService()?.getFsNodeByPath("/");

        // TODO: Implement root querying
        result.newRow().apply {
            add(DocumentsContract.Root.COLUMN_ROOT_ID, "orbit-root")
            add(DocumentsContract.Root.COLUMN_FLAGS,
                DocumentsContract.Root.FLAG_SUPPORTS_CREATE or
                DocumentsContract.Root.FLAG_SUPPORTS_IS_CHILD)
            add(DocumentsContract.Root.COLUMN_ICON, R.mipmap.ic_launcher)
            add(DocumentsContract.Root.COLUMN_TITLE, context?.getString(R.string.app_name))
            add(DocumentsContract.Root.COLUMN_DOCUMENT_ID, "/")
        }

        return result
    }

    override fun queryDocument(documentId: String?, projection: Array<out String>?): Cursor {
        Log.d(TAG, "queryDocument: $documentId")
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)

        val fsNodeInfo = orbitService?.getService()?.getFsNodeByPath(documentId!!);
        if (fsNodeInfo == null) {
            return result
        }

        val flags = if (fsNodeInfo.kind == FileKind.DIRECTORY) {
            DocumentsContract.Document.FLAG_DIR_SUPPORTS_CREATE
        } else {
            DocumentsContract.Document.FLAG_SUPPORTS_WRITE or
            DocumentsContract.Document.FLAG_SUPPORTS_DELETE or
            DocumentsContract.Document.FLAG_SUPPORTS_RENAME
        }

        val filename = documentId!!.split("/").last()

        result.newRow()
            .add(DocumentsContract.Document.COLUMN_DOCUMENT_ID, documentId!!)
            .add(DocumentsContract.Document.COLUMN_DISPLAY_NAME, filename)
            .add(DocumentsContract.Document.COLUMN_SIZE, fsNodeInfo.size)
            .add(DocumentsContract.Document.COLUMN_MIME_TYPE, if (fsNodeInfo.kind == FileKind.DIRECTORY) {
                DocumentsContract.Document.MIME_TYPE_DIR } else { getMimeType(filename) })
            .add(DocumentsContract.Document.COLUMN_FLAGS, flags)
            .add(DocumentsContract.Document.COLUMN_LAST_MODIFIED, fsNodeInfo.modificationTimeMs)
        return result
    }

    override fun queryChildDocuments(
        parentDocumentId: String?,
        projection: Array<out String>?,
        sortOrder: String?
    ): Cursor {
        Log.d(TAG, "queryChildDocuments: $parentDocumentId")
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        val parentId = parentDocumentId ?: "/"

        val entries = orbitService?.getService()?.getDirectoryEntries(parentId) ?: emptyList()
        for (entry in entries) {
            Log.d(TAG, "found child entry: $entry")
            val flags = if (entry.kind == FileKind.DIRECTORY) {
                DocumentsContract.Document.FLAG_DIR_SUPPORTS_CREATE
            } else {
                DocumentsContract.Document.FLAG_SUPPORTS_WRITE or
                DocumentsContract.Document.FLAG_SUPPORTS_DELETE or
                DocumentsContract.Document.FLAG_SUPPORTS_RENAME
            }

            val documentId = if (parentId == "/") {
                "/${entry.name}"
            } else {
                "$parentId/${entry.name}"
            }

            result.newRow()
                .add(DocumentsContract.Document.COLUMN_DOCUMENT_ID, documentId)
                .add(DocumentsContract.Document.COLUMN_SIZE, entry.size)
                .add(DocumentsContract.Document.COLUMN_DISPLAY_NAME, entry.name)
                .add(DocumentsContract.Document.COLUMN_MIME_TYPE, if (entry.kind == FileKind.DIRECTORY) {
                    DocumentsContract.Document.MIME_TYPE_DIR } else { getMimeType(entry.name) })
                .add(DocumentsContract.Document.COLUMN_FLAGS, flags)
                .add(DocumentsContract.Document.COLUMN_LAST_MODIFIED, entry.modificationTimeMs)
        }
        return result
    }

    override fun openDocument(
        documentId: String?,
        mode: String?,
        signal: CancellationSignal?
    ): ParcelFileDescriptor {
        Log.d(TAG, "openDocument: $documentId, mode: $mode")

        if (documentId == null) {
            throw FileNotFoundException("Document ID is null")
        }

        val service = orbitService?.getService()
            ?: throw FileNotFoundException("Orbit service not available")

        // Get the backing file path from Orbit
        val backingFilePath = service.getBackingFilePath(documentId)

        val backingFile = java.io.File(backingFilePath)
        val accessMode = ParcelFileDescriptor.parseMode(mode)
        val isWriteMode = mode?.contains('w') ?: false
        Log.d(TAG, "Acces is writable = $isWriteMode")

        // If file doesn't exist locally and we're in read mode, request it from network
        if (!backingFile.exists() && !isWriteMode) {
            Log.d(TAG, "Backing file not found, requesting from network: $backingFilePath")

            // Create a pipe for streaming the file content
            val pipe = ParcelFileDescriptor.createPipe()
            val readEnd = pipe[0]
            val writeEnd = pipe[1]

            // Request file from network in background thread
            Thread {
                try {
                    service.requestFile(documentId, 30u, object : FileRequestCallback {
                        override fun onComplete(result: FileRequestResult) {
                            try {
                                when (result) {
                                    FileRequestResult.SUCCESS -> {
                                        Log.d(TAG, "File received from network, writing to pipe")
                                        // File received, copy backing file content to pipe
                                        val file = java.io.File(backingFilePath)
                                        file.inputStream().use { input ->
                                            ParcelFileDescriptor.AutoCloseOutputStream(writeEnd).use { output ->
                                                input.copyTo(output)
                                            }
                                        }
                                        Log.d(TAG, "Successfully wrote file content to pipe")
                                    }
                                    FileRequestResult.TIMEOUT -> {
                                        // Timeout, close the pipe to signal error
                                        Log.w(TAG, "Timed out waiting for file from network")
                                        writeEnd.close()
                                    }
                                }
                            } catch (e: Exception) {
                                Log.e(TAG, "Error handling file request result: ${e.message}", e)
                                try {
                                    writeEnd.close()
                                } catch (closeException: Exception) {
                                    Log.e(TAG, "Error closing write end: ${closeException.message}")
                                }
                            }
                        }
                    })
                } catch (e: Exception) {
                    Log.e(TAG, "Error requesting file: ${e.message}", e)
                    try {
                        writeEnd.close()
                    } catch (closeException: Exception) {
                        Log.e(TAG, "Error closing write end: ${closeException.message}")
                    }
                }
            }.start()

            return readEnd
        }

        // If file doesn't exist and we're in write mode, we can't handle this yet
        if (!backingFile.exists()) {
            throw FileNotFoundException("Backing file not found: $backingFilePath")
        }

        // For read-only access with existing file, return the backing file directly
        if (!isWriteMode) {
            return ParcelFileDescriptor.open(backingFile, accessMode)
        }

        val tempFile = java.io.File.createTempFile("orbit_", ".tmp", context?.cacheDir)
        Log.d(TAG, "Created temp file for writing: ${tempFile.absolutePath}")

        // Copy current content to temp file
        backingFile.inputStream().use { input ->
            tempFile.outputStream().use { output ->
                input.copyTo(output)
            }
        }

        // Open the temp file with write access
        val pfd = ParcelFileDescriptor.open(tempFile, accessMode, Handler(Looper.getMainLooper())) {
            e ->
            Log.d(TAG, "Temp file closed, updating Orbit: $documentId")
            try {
                service.updateFileFrom(documentId, tempFile.absolutePath)
                Log.d(TAG, "Successfully updated file in Orbit")

                // Notify Android that the document has changed
                val uri = DocumentsContract.buildDocumentUri(AUTHORITY, documentId)
                context?.contentResolver?.notifyChange(uri, null)
                Log.d(TAG, "Notified content change for: $uri")
            } catch (ex: Exception) {
                Log.e(TAG, "Failed to update file in Orbit: ${ex.message}", ex)
            } finally {
                // Clean up temp file
                if (tempFile.exists()) {
                    tempFile.delete()
                    Log.d(TAG, "Deleted temp file: ${tempFile.absolutePath}")
                }
            }
        }


        return pfd
    }

    override fun createDocument(
        parentDocumentId: String?,
        mimeType: String?,
        displayName: String?
    ): String? {
        Log.d(TAG, "createDocument: parent=$parentDocumentId, mimeType=$mimeType, name=$displayName")

        if (displayName == null || displayName.isEmpty()) {
            throw IllegalArgumentException("Display name is required")
        }

        val parentPath = parentDocumentId ?: "/"

        // Create the file in Orbit
        try {
            orbitService?.getService()?.createFile(parentPath, displayName)
        } catch (e: uniffi.orbit_android.OrbitException.FileExists) {
            Log.w(TAG, "File already exists: $displayName")
            throw IllegalArgumentException("File already exists: $displayName")
        } catch (e: uniffi.orbit_android.OrbitException.PathNotFound) {
            Log.e(TAG, "Parent directory not found: $parentPath", e)
            throw java.io.FileNotFoundException("Parent directory not found: $parentPath")
        } catch (e: uniffi.orbit_android.OrbitException.NotADirectory) {
            Log.e(TAG, "Parent path is not a directory: $parentPath", e)
            throw java.io.FileNotFoundException("Parent path is not a directory: $parentPath")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to create file: ${e.message}", e)
            throw java.io.IOException("Failed to create file: ${e.message}")
        }

        // Return the new document ID (which is the full path)
        val newDocumentId = if (parentPath == "/") {
            "/$displayName"
        } else {
            "$parentPath/$displayName"
        }

        Log.d(TAG, "Created document with ID: $newDocumentId")
        return newDocumentId
    }

    override fun deleteDocument(documentId: String?) {
        Log.d(TAG, "deleteDocument: $documentId")

        // TODO: Implement document deletion
    }

    override fun renameDocument(documentId: String?, displayName: String?): String? {
        Log.d(TAG, "renameDocument: $documentId to $displayName")

        // TODO: Implement document renaming
        return null
    }

    override fun isChildDocument(parentDocumentId: String?, documentId: String?): Boolean {
        Log.d(TAG, "isChildDocument: parent=$parentDocumentId, document=$documentId")

        // TODO: Implement child document checking
        return false
    }

    override fun shutdown() {
        Log.d(TAG, "shutdown")
        super.shutdown()
        context?.unbindService(serviceConnection)
    }

    private fun getMimeType(filename: String): String {
        val extension = filename.substringAfterLast('.', "")
        return if (extension.isEmpty()) {
            "application/octet-stream"
        } else {
            MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)
                ?: "application/octet-stream"
        }
    }
}
