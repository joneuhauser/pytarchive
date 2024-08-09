import configparser
from pytarchive.service.utils import singleton


@singleton
class ConfigReader:
    def __init__(self, config_path="/etc/pytarchive/pytarchive.conf"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.read_config()

    def read_config(self, default=None):
        try:
            self.config.read(self.config_path)
        except Exception as e:
            if default is not None:
                return default
            raise RuntimeError(f"Failed to read configuration file: {e}")

    def get(self, section, attribute, default=None):
        try:
            return self.config.get(section, attribute)
        except Exception as e:
            print(e)
            return default

    def get_drive_serial(self):
        return self.get("Device", "drive_serial")

    def get_library_path(self):
        return self.get("Device", "library")

    def get_maxsize(self):
        return int(self.get("General", "tape_max_size"))

    def get_exclude_folders(self):
        entry = self.get("General", "exclude_folders")
        return [e.strip() for e in entry.split(",")]

    def get_source_folders(self):
        entry = self.get("General", "source_folders")
        return [e.strip() for e in entry.split(",")]
