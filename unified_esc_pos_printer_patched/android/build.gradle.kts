group = "com.elriztechnology.unified_esc_pos_printer"
version = "1.0"

plugins {
    id("com.android.library")
}

android {
    namespace = "com.elriztechnology.unified_esc_pos_printer"
    compileSdk = 36

    defaultConfig {
        minSdk = 21
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}

dependencies {
    implementation("androidx.core:core-ktx:1.12.0")
}