package de.maufl.orbit

import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.tooling.preview.Preview
import de.maufl.orbit.ui.theme.OrbitTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        // Start the OrbitService
        startService(Intent(this, OrbitService::class.java))

        // Schedule periodic work to keep service running
        OrbitServiceScheduler.schedulePeriodicWork(this)

        setContent {
            OrbitTheme {
                FileBrowserScreen()
            }
        }
    }
}