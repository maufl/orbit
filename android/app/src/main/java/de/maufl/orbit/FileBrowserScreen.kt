package de.maufl.orbit

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.IBinder
import androidx.compose.foundation.Image
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import de.maufl.orbit.R
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material.icons.filled.InsertDriveFile
import androidx.compose.material.icons.filled.Folder
import androidx.compose.material.icons.filled.Menu
import androidx.compose.material3.*
import kotlinx.coroutines.launch
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import uniffi.orbit_android.DirectoryEntryInfo
import uniffi.orbit_android.FileKind
import uniffi.orbit_android.RootChangeCallback

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun FileBrowserScreen() {
    val context = LocalContext.current
    val drawerState = rememberDrawerState(initialValue = DrawerValue.Closed)
    val scope = rememberCoroutineScope()

    var orbitService by remember { mutableStateOf<OrbitService?>(null) }
    var currentPath by remember { mutableStateOf("/") }
    var entries by remember { mutableStateOf<List<DirectoryEntryInfo>>(emptyList()) }
    var isLoading by remember { mutableStateOf(true) }
    var error by remember { mutableStateOf<String?>(null) }
    var refreshTrigger by remember { mutableStateOf(0) }

    // Bind to OrbitService
    DisposableEffect(context) {
        val connection = object : ServiceConnection {
            override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
                val binder = service as OrbitService.OrbitBinder
                orbitService = binder.getService()

                // Register root change callback
                orbitService?.registerRootChangeCallback(object : RootChangeCallback {
                    override fun onRootChanged() {
                        refreshTrigger++
                    }
                })

                loadDirectory(orbitService, currentPath) { newEntries, errorMsg ->
                    entries = newEntries
                    error = errorMsg
                    isLoading = false
                }
            }

            override fun onServiceDisconnected(name: ComponentName?) {
                orbitService = null
            }
        }

        val intent = Intent(context, OrbitService::class.java)
        context.bindService(intent, connection, Context.BIND_AUTO_CREATE)

        onDispose {
            context.unbindService(connection)
        }
    }

    // Reload directory when path changes or root changes
    LaunchedEffect(currentPath, orbitService, refreshTrigger) {
        orbitService?.let { service ->
            isLoading = true
            loadDirectory(service, currentPath) { newEntries, errorMsg ->
                entries = newEntries
                error = errorMsg
                isLoading = false
            }
        }
    }

    ModalNavigationDrawer(
        drawerState = drawerState,
        drawerContent = {
            ModalDrawerSheet {
                Row(
                    modifier = Modifier.padding(16.dp),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(12.dp)
                ) {
                    Image(
                        painter = painterResource(id = R.drawable.orbit),
                        contentDescription = "Orbit icon",
                        modifier = Modifier.size(40.dp)
                    )
                    Text(
                        text = "Orbit",
                        style = MaterialTheme.typography.titleLarge
                    )
                }
                HorizontalDivider()
                NavigationDrawerItem(
                    label = { Text("Files") },
                    selected = true,
                    onClick = {
                        scope.launch {
                            drawerState.close()
                        }
                    }
                )
                // Add more menu items here in the future
            }
        }
    ) {
        Scaffold(
            topBar = {
                TopAppBar(
                    title = { Text(text = currentPath) },
                    navigationIcon = {
                        if (currentPath != "/") {
                            IconButton(onClick = {
                                currentPath = currentPath.substringBeforeLast("/").ifEmpty { "/" }
                            }) {
                                Icon(Icons.AutoMirrored.Filled.ArrowBack, "Navigate up")
                            }
                        } else {
                            IconButton(onClick = {
                                scope.launch {
                                    drawerState.open()
                                }
                            }) {
                                Icon(Icons.Default.Menu, "Open menu")
                            }
                        }
                    }
                )
            }
        ) { paddingValues ->
        Box(
            modifier = Modifier
                .fillMaxSize()
                .padding(paddingValues)
        ) {
            when {
                isLoading -> {
                    CircularProgressIndicator(
                        modifier = Modifier.align(Alignment.Center)
                    )
                }
                error != null -> {
                    Text(
                        text = "Error: $error",
                        color = MaterialTheme.colorScheme.error,
                        modifier = Modifier
                            .align(Alignment.Center)
                            .padding(16.dp)
                    )
                }
                entries.isEmpty() -> {
                    Text(
                        text = "Empty directory",
                        modifier = Modifier
                            .align(Alignment.Center)
                            .padding(16.dp)
                    )
                }
                else -> {
                    LazyColumn {
                        items(entries) { entry ->
                            FileListItem(
                                entry = entry,
                                onClick = {
                                    if (entry.kind == FileKind.DIRECTORY) {
                                        currentPath = if (currentPath == "/") {
                                            "/${entry.name}"
                                        } else {
                                            "$currentPath/${entry.name}"
                                        }
                                    }
                                }
                            )
                        }
                    }
                }
            }
        }
        }
    }
}

@Composable
fun FileListItem(
    entry: DirectoryEntryInfo,
    onClick: () -> Unit
) {
    val isDirectory = entry.kind == FileKind.DIRECTORY

    ListItem(
        headlineContent = {
            Text(
                text = entry.name,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        },
        supportingContent = {
            if (!isDirectory) {
                Text(text = formatFileSize(entry.size))
            }
        },
        leadingContent = {
            Icon(
                imageVector = if (isDirectory) {
                    Icons.Default.Folder
                } else {
                    Icons.Default.InsertDriveFile
                },
                contentDescription = if (isDirectory) "Folder" else "File",
                tint = if (isDirectory) {
                    MaterialTheme.colorScheme.primary
                } else {
                    MaterialTheme.colorScheme.onSurfaceVariant
                }
            )
        },
        modifier = Modifier.clickable(onClick = onClick)
    )
    HorizontalDivider()
}

private fun loadDirectory(
    service: OrbitService?,
    path: String,
    onResult: (List<DirectoryEntryInfo>, String?) -> Unit
) {
    try {
        val entries = service?.getDirectoryEntries(path) ?: emptyList()
        onResult(entries, null)
    } catch (e: Exception) {
        onResult(emptyList(), e.message)
    }
}

private fun formatFileSize(bytes: ULong): String {
    val kb = 1024.0
    val mb = kb * 1024
    val gb = mb * 1024

    val bytesDouble = bytes.toDouble()

    return when {
        bytes < 1024uL -> "$bytes B"
        bytesDouble < mb -> "%.1f KB".format(bytesDouble / kb)
        bytesDouble < gb -> "%.1f MB".format(bytesDouble / mb)
        else -> "%.1f GB".format(bytesDouble / gb)
    }
}
