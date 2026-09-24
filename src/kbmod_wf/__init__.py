def __getattr__(name):
    # Resource discovery belongs to the selected workflow, not package import.
    if name == "workflow_runner":
        from .workflow import workflow_runner

        return workflow_runner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
