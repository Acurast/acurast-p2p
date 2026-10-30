# Build environment of the Android library: JDK 17, Android SDK + NDK, Rust with the Android
# targets (installed from rust-toolchain.toml).
FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install --assume-yes --no-install-recommends \
    build-essential curl git python3 unzip \
    && rm -rf /var/lib/apt/lists/*

ENV ANDROID_HOME=/opt/android-sdk
RUN mkdir -p $ANDROID_HOME \
    && curl -sSfL -o /tmp/cmdline-tools.zip https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip \
    && unzip -q /tmp/cmdline-tools.zip -d $ANDROID_HOME \
    && rm /tmp/cmdline-tools.zip \
    && yes | $ANDROID_HOME/cmdline-tools/bin/sdkmanager --sdk_root=$ANDROID_HOME --licenses > /dev/null \
    && $ANDROID_HOME/cmdline-tools/bin/sdkmanager --sdk_root=$ANDROID_HOME \
        "platforms;android-35" "build-tools;35.0.0" "ndk;29.0.14206865"

ENV PATH=/root/.cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none

WORKDIR /app
COPY rust-toolchain.toml .
RUN rustup show

COPY . .
RUN echo "sdk.dir=$ANDROID_HOME" > android/local.properties
