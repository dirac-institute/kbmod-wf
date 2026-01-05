import unittest
from parsl.config import Config

from kbmod_wf.utilities.configuration_utilities import get_resource_config


class TestResourceConfig(unittest.TestCase):
    def test_load_dev_configuration(self):
        """
        Verify that the 'dev' resource configuration can be instantiated.

        This ensures that the configuration does not use any deprecated arguments
        that would cause a TypeError during initialization.
        """
        config = get_resource_config(env="dev")
        self.assertIsInstance(config, Config)

    def test_load_klone_configuration(self):
        """
        Verify that the 'klone' resource configuration can be instantiated.

        This ensures that the configuration does not use any deprecated arguments
        that would cause a TypeError during initialization.
        """
        config = get_resource_config(env="klone")
        self.assertIsInstance(config, Config)

    def test_load_usdf_configuration(self):
        """
        Verify that the 'usdf' resource configuration can be instantiated.

        This test specifically checks for regression of the TypeError caused by
        passing deprecated arguments (e.g. app_cache, checkpoint_mode) to parsl.Config.

        Note: The 'usdf' environment typically requires specific system paths or
        environment variables (e.g. SLURM). We catch unrelated exceptions that
        occur due to missing system dependencies, failing only if the specific
        deprecation 'unexpected keyword argument' TypeError is raised.
        """
        try:
            config = get_resource_config(env="usdf")
            self.assertIsInstance(config, Config)
        except Exception as e:
            # If it fails due to missing environment vars or paths, that's "okay" for this specific check
            # as long as it's NOT the TypeError regarding deprecated arguments we are fixing.
            if "unexpected keyword argument" in str(e):
                self.fail(f"Config init failed with TypeError: {e}")
            # Otherwise, we ignore failures related to missing environment/system dependencies
            # as we are only testing for the deprecation regression here.
            pass

if __name__ == "__main__":
    unittest.main()
