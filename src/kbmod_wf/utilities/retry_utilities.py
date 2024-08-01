def klone_retry_handler(exception, task_record):
    """Custom retry handler for Klone, that will not increment the retry count
    when resources are preempted. I.e. if we lose access to resources, don't
    count that as a failure, and don't increase the retry counter.

    Parameters
    ----------
    exception : Exception
        The exception that was raised during the task execution.
    task_record : TaskRecord
        The parsl TaskRecord for the task that failed.

    Returns
    -------
    int
        The amount by which to increment the retry counter.
    """

    if isinstance(exception, Exception):
        return 0
    else:
        return 1
