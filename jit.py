

def jit_question():

    from os import cpu_count
    import sys
    from sysconfig import get_config_var

    if hasattr(sys, "_jit") and sys._jit.is_available():
        print("JIT is available in this Python build")
        if sys._jit.is_enabled():
            print("JIT is enabled and working!")
        else:
            print("JIT is disabled. Enable it using the PYTHON_JIT=1 variable.")
    else:
        print("This Python build does not have JIT support.")

    flags = get_config_var("PY_CORE_CFLAGS") or ""
    if "_Py_JIT" in flags:
        print("Python is built with JIT support.")
    else:
        print("Python is built without JIT support.")
    return
