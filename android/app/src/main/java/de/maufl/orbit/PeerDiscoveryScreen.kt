package de.maufl.orbit

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.IBinder
import android.util.Log
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Devices
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import uniffi.orbit_android.PeerDiscoveryCallback
import uniffi.orbit_android.PeerInfo
import java.text.SimpleDateFormat
import java.util.*

@Composable
fun PeerDiscoveryScreen() {
    val context = LocalContext.current
    var orbitService by remember { mutableStateOf<OrbitService?>(null) }
    var discoveredPeers by remember { mutableStateOf<Map<String, PeerInfo>>(emptyMap()) }
    var knownPeers by remember { mutableStateOf<Set<String>>(emptySet()) }

    // Bind to OrbitService
    DisposableEffect(context) {
        val connection = object : ServiceConnection {
            override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
                Log.d("PeerDiscoveryScreen", "Connected to OrbitService")
                val binder = service as OrbitService.OrbitBinder
                orbitService = binder.getService()

                // Load known peers
                orbitService?.let {
                    knownPeers = it.getKnownPeers().toSet()
                    Log.d("PeerDiscoveryScreen", "Loaded ${knownPeers.size} known peers")
                }

                // Register peer discovery callback
                Log.d("PeerDiscoveryScreen", "Registering peer discovery callback")
                orbitService?.registerPeerDiscoveryCallback(object : PeerDiscoveryCallback {
                    override fun onPeerDiscovered(peer: PeerInfo) {
                        Log.d("PeerDiscoveryScreen", "Peer discovered: ${peer.nodeId}")
                        discoveredPeers = discoveredPeers + (peer.nodeId to peer)
                    }

                    override fun onPeerExpired(nodeId: String) {
                        Log.d("PeerDiscoveryScreen", "Peer expired: $nodeId")
                        discoveredPeers = discoveredPeers - nodeId
                    }
                })
                Log.d("PeerDiscoveryScreen", "Peer discovery callback registered")
            }

            override fun onServiceDisconnected(name: ComponentName?) {
                Log.d("PeerDiscoveryScreen", "Disconnected from OrbitService")
                orbitService = null
            }
        }

        val intent = Intent(context, OrbitService::class.java)
        context.bindService(intent, connection, Context.BIND_AUTO_CREATE)

        onDispose {
            context.unbindService(connection)
        }
    }

    Box(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
    ) {
        when {
            discoveredPeers.isEmpty() -> {
                Column(
                    modifier = Modifier.align(Alignment.Center),
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    Icon(
                        imageVector = Icons.Default.Devices,
                        contentDescription = "No peers",
                        modifier = Modifier.size(64.dp),
                        tint = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(modifier = Modifier.height(16.dp))
                    Text(
                        text = "No peers discovered",
                        style = MaterialTheme.typography.titleMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(modifier = Modifier.height(8.dp))
                    Text(
                        text = "Make sure other Orbit devices are on the same network",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
            else -> {
                LazyColumn {
                    items(discoveredPeers.values.toList()) { peer ->
                        PeerListItem(
                            peer = peer,
                            isKnownPeer = knownPeers.contains(peer.nodeId),
                            onAddPeer = { nodeId ->
                                orbitService?.addPeer(nodeId)
                                knownPeers = knownPeers + nodeId
                                Log.i("PeerDiscoveryScreen", "Added peer: $nodeId")
                            }
                        )
                    }
                }
            }
        }
    }
}

@Composable
fun PeerListItem(
    peer: PeerInfo,
    isKnownPeer: Boolean,
    onAddPeer: (String) -> Unit
) {
    ListItem(
        headlineContent = {
            Text(
                text = peer.nodeName ?: formatNodeId(peer.nodeId),
                style = MaterialTheme.typography.bodyLarge
            )
        },
        supportingContent = {
            Column {
                // Show node ID if we have a name
                peer.nodeName?.let {
                    Text(
                        text = formatNodeId(peer.nodeId),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                // Show last seen timestamp
                peer.lastSeenMs?.let { timestamp ->
                    Text(
                        text = "Last seen: ${formatTimestamp(timestamp)}",
                        style = MaterialTheme.typography.bodyMedium
                    )
                }
            }
        },
        leadingContent = {
            Icon(
                imageVector = Icons.Default.Devices,
                contentDescription = "Peer",
                tint = MaterialTheme.colorScheme.primary
            )
        },
        trailingContent = {
            if (isKnownPeer) {
                Text(
                    text = "Added",
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.primary
                )
            } else {
                Button(onClick = { onAddPeer(peer.nodeId) }) {
                    Text("Add")
                }
            }
        }
    )
    HorizontalDivider()
}

private fun formatNodeId(nodeId: String): String {
    // Show first 8 and last 8 characters for readability
    return if (nodeId.length > 16) {
        "${nodeId.take(8)}...${nodeId.takeLast(8)}"
    } else {
        nodeId
    }
}

private fun formatTimestamp(timestampMs: Long): String {
    val dateFormat = SimpleDateFormat("HH:mm:ss", Locale.getDefault())
    return dateFormat.format(Date(timestampMs))
}
