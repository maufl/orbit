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
import android.os.IBinder
import android.os.ParcelFileDescriptor
import android.provider.DocumentsContract
import android.provider.DocumentsProvider
import android.util.Log
import android.webkit.MimeTypeMap
import uniffi.orbit_android.FileKind
import java.io.FileNotFoundException

class OrbitDocumentProvider : DocumentsProvider() {

    companion object {
        private const val TAG = "OrbitDocumentProvider"
        private val DEFAULT_ROOT_PROJECTION = arrayOf(
            DocumentsContract.Root.COLUMN_ROOT_ID,
            DocumentsContract.Root.COLUMN_MIME_TYPES,
            DocumentsContract.Root.COLUMN_FLAGS,
            DocumentsContract.Root.COLUMN_ICON,
            DocumentsContract.Root.COLUMN_TITLE,
            DocumentsContract.Root.COLUMN_SUMMARY,
            DocumentsContract.Root.COLUMN_DOCUMENT_ID,
            DocumentsContract.Root.COLUMN_AVAILABLE_BYTES
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
            add(DocumentsContract.Root.COLUMN_MIME_TYPES, "*/*")
            add(DocumentsContract.Root.COLUMN_FLAGS,
                DocumentsContract.Root.FLAG_SUPPORTS_CREATE or
                DocumentsContract.Root.FLAG_SUPPORTS_IS_CHILD)
            add(DocumentsContract.Root.COLUMN_ICON, R.mipmap.ic_launcher)
            add(DocumentsContract.Root.COLUMN_TITLE, context?.getString(R.string.app_name))
            add(DocumentsContract.Root.COLUMN_SUMMARY, null)
            add(DocumentsContract.Root.COLUMN_DOCUMENT_ID, "/")
            add(DocumentsContract.Root.COLUMN_AVAILABLE_BYTES, null)
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

        result.newRow()
            .add(DocumentsContract.Document.COLUMN_DOCUMENT_ID, documentId!!)
            .add(DocumentsContract.Document.COLUMN_DISPLAY_NAME, documentId!!.split("/").last())
            .add(DocumentsContract.Document.COLUMN_SIZE, fsNodeInfo.size)
            .add(DocumentsContract.Document.COLUMN_MIME_TYPE, if (fsNodeInfo.kind == FileKind.DIRECTORY) {
                DocumentsContract.Document.MIME_TYPE_DIR } else { "*/*" })
            .add(DocumentsContract.Document.COLUMN_FLAGS, flags)
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
                    DocumentsContract.Document.MIME_TYPE_DIR } else { "*/*" })
                .add(DocumentsContract.Document.COLUMN_FLAGS, flags)
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

        // Get the backing file path from Orbit
        val backingFilePath = orbitService?.getService()?.getBackingFilePath(documentId)
            ?: throw FileNotFoundException("Orbit service not available")

        val file = java.io.File(backingFilePath)
        if (!file.exists()) {
            throw FileNotFoundException("Backing file not found: $backingFilePath")
        }

        val accessMode = ParcelFileDescriptor.parseMode(mode)
        return ParcelFileDescriptor.open(file, accessMode)
    }

    override fun createDocument(
        parentDocumentId: String?,
        mimeType: String?,
        displayName: String?
    ): String? {
        Log.d(TAG, "createDocument: parent=$parentDocumentId, mimeType=$mimeType, name=$displayName")

        if (displayName == null || displayName.isEmpty()) {
            Log.e(TAG, "Display name is null or empty")
            return null
        }

        val parentPath = parentDocumentId ?: "/"

        // Create the file in Orbit
        try {
            orbitService?.getService()?.createFile(parentPath, displayName)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to create file: ${e.message}", e)
            return null
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
