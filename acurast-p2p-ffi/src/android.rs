//! Android context for the crates that call into the JVM: iroh reads the system's DNS servers
//! through it and falls back to public resolvers without it.

use std::sync::Once;

use jni_sys::{JNI_OK, JNIEnv, JavaVM, jclass, jobject};

static INSTALL: Once = Once::new();

/// `com.acurast.p2p.AcurastP2pAndroid.installContext(Context)`: keeps the Java VM and a global
/// reference to the application context for the rest of the process. Only the first call counts.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_acurast_p2p_AcurastP2pAndroid_installContext(
    env: *mut JNIEnv,
    _class: jclass,
    context: jobject,
) {
    INSTALL.call_once(|| unsafe { install(env, context) });
}

unsafe fn install(env: *mut JNIEnv, context: jobject) {
    let (Some(get_java_vm), Some(new_global_ref)) = (unsafe { (**env).GetJavaVM }, unsafe {
        (**env).NewGlobalRef
    }) else {
        return;
    };
    let mut vm: *mut JavaVM = std::ptr::null_mut();
    if unsafe { get_java_vm(env, &mut vm) } != JNI_OK || vm.is_null() {
        return;
    }
    // Never released: the context must stay valid until the process exits.
    let context = unsafe { new_global_ref(env, context) };
    if context.is_null() {
        return;
    }
    unsafe { ndk_context::initialize_android_context(vm.cast(), context.cast()) };
}
