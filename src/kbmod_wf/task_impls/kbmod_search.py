import time


def kbmod_search(input_wu=None, result_file=None, logger=None):
    logger.info("In the kbmod_search task_impl")
    with open(input_wu, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    time.sleep(5)

    with open(result_file, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return result_file
