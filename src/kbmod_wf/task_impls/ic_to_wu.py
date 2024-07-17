import time


def ic_to_wu(ic_file=None, wu_file=None, logger=None):
    logger.info("In the ic_to_wu task_impl")
    with open(ic_file, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(wu_file, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return wu_file
