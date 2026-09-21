def __getattr__(name):
    # Native FITS preparation does not require Butler, CUDA, or injection tools.
    if name in {"ic_to_wu", "kbmod_search", "uri_to_ic"}:
        from importlib import import_module

        value = getattr(import_module(f"{__name__}.{name}"), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ic_to_wu",
    "kbmod_search",
    "reproject_wu",
    "uri_to_ic",
]
