plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
    id("org.mozilla.rust-android-gradle.rust-android")
}

android {
    namespace = "de.maufl.orbit"
    compileSdk = 36
    System.getenv("ANDROID_NDK")?.let { ndk ->
        ndkPath = ndk
        // Read NDK version from source.properties
        val propsFile = File(ndk, "source.properties")
        if (propsFile.exists()) {
            propsFile.readLines()
                .firstOrNull { it.startsWith("Pkg.Revision") }
                ?.substringAfter("=")
                ?.trim()
                ?.let { ndkVersion = it }
        }
    }

    defaultConfig {
        applicationId = "de.maufl.orbit"
        minSdk = 24
        targetSdk = 36
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
    buildFeatures {
        compose = true
    }
}

// Rust build configuration
val rustTargets = listOf("arm64", "x86_64")  // Android architectures
val rustProfile = "debug"  // Use "debug" for development, "release" for production

// Map Gradle target names to Rust target triples
val targetToRustTriple = mapOf(
    "arm64" to "aarch64-linux-android",
    "x86_64" to "x86_64-linux-android"
)

cargo {
    module = "../../orbit-android"  // Relative path to Cargo.toml directory
    libname = "orbit_android"       // Must match the [lib] name in Cargo.toml
    targets = rustTargets
    profile = rustProfile
    targetDirectory = "../../target" // Shared target directory
}

// Fix Python 3.13 compatibility in linker-wrapper
val fixLinkerWrapper = tasks.register("fixLinkerWrapper") {
    description = "Fix Python 3.13 compatibility in generated linker-wrapper"
    group = "build"

    doLast {
        val wrapperFile = file("${project.rootDir}/build/linker-wrapper/linker-wrapper.py")
        if (wrapperFile.exists()) {
            val content = wrapperFile.readText()
            if (content.contains("import pipes")) {
                val fixedContent = content.replace("import pipes", "import shlex")
                    .replace("pipes.quote", "shlex.quote")
                wrapperFile.writeText(fixedContent)
                println("Fixed linker-wrapper.py for Python 3.13 compatibility")
            }
        }
    }
}

// Ensure linker wrapper is fixed before cargo builds
tasks.matching { it.name.startsWith("cargoBuild") }.configureEach {
    dependsOn(fixLinkerWrapper)
}

// Generate UniFFI bindings
val generateUniFFIBindings = tasks.register<Exec>("generateUniFFIBindings") {
    description = "Generate Kotlin bindings from Rust using UniFFI"
    group = "build"

    workingDir = file("${project.rootDir}/../")

    // Build the library first
    dependsOn("cargoBuild")

    // Find any available .so file (bindings are the same for all targets)
    doFirst {
        val targetDir = file("${project.rootDir}/../target")
        val soFile = rustTargets
            .mapNotNull { targetToRustTriple[it] }
            .map { File(targetDir, "$it/$rustProfile/liborbit_android.so") }
            .firstOrNull { it.exists() }
            ?: throw GradleException("No built library found. Run cargoBuild first.")

        commandLine(
            "cargo", "run", "-p", "uniffi-bindgen", "generate",
            "--library", soFile.absolutePath,
            "--language", "kotlin",
            "--out-dir", "${project.projectDir}/src/main/java/",
            "--no-format"
        )
    }
}

// Make sure bindings are generated before Kotlin compilation
tasks.named("preBuild") {
    dependsOn(generateUniFFIBindings)
}

dependencies {

    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.activity.compose)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.ui)
    implementation(libs.androidx.ui.graphics)
    implementation(libs.androidx.ui.tooling.preview)
    implementation(libs.androidx.material3)
    implementation("androidx.compose.material:material-icons-extended:1.7.6")
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.ui.test.junit4)
    debugImplementation(libs.androidx.ui.tooling)
    debugImplementation(libs.androidx.ui.test.manifest)
    // JNA dependency for ffi
    implementation("net.java.dev.jna:jna:5.12.0@aar")
}