import time


def reproject_wu(input_wu=None, reprojected_wu=None, logger=None):
    logger.info("In the reproject_wu task_impl")
    with open(input_wu, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(reprojected_wu, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return reprojected_wu
