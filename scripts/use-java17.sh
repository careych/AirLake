#!/bin/sh
# Source from bash or zsh on macOS. Other systems must set JAVA_HOME manually.

_airlake_use_java17() {
    if [ "$(uname -s)" != "Darwin" ]; then
        printf '%s\n' 'This helper supports macOS only; set JAVA_HOME to Java 17 manually.' >&2
        return 1
    fi

    _airlake_java_home=${JAVA_HOME:-}
    if [ -z "$_airlake_java_home" ]; then
        # -F prevents java_home from silently falling back to a different JDK.
        _airlake_java_home=$(/usr/libexec/java_home -F -v 17 2>/dev/null) || _airlake_java_home=
    fi
    if [ -z "$_airlake_java_home" ] && command -v brew >/dev/null 2>&1; then
        _airlake_java_prefix=$(brew --prefix openjdk@17) || return 1
        _airlake_java_home="$_airlake_java_prefix/libexec/openjdk.jdk/Contents/Home"
    fi
    if [ ! -x "$_airlake_java_home/bin/java" ]; then
        printf '%s\n' 'Java 17 was not found. Install a JDK or set JAVA_HOME, then source this helper again.' >&2
        return 1
    fi

    _airlake_java_version=$("$_airlake_java_home/bin/java" -version 2>&1) || return 1
    case "$_airlake_java_version" in
        *'version "17.'*|*'version "17"'*) ;;
        *) printf '%s\n' 'JAVA_HOME must point to Java 17.' >&2; return 1 ;;
    esac

    export JAVA_HOME="$_airlake_java_home"
    export PATH="$JAVA_HOME/bin:$PATH"
    printf '%s\n' "$_airlake_java_version"
}

_airlake_use_java17
