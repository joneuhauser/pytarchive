import configparser


def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


@singleton
class ConfigReader:
    def __init__(self, config_path="/etc/pyarchive.conf"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.read_config()

    def read_config(self):
        try:
            self.config.read(self.config_path)
        except Exception as e:
            raise RuntimeError(f"Failed to read configuration file: {e}")

    def get(self, section, attribute):
        return self.config.get(section, attribute)

    def get_drive_serial(self):
        return self.get("Device", "drive_serial")

    def get_library_path(self):
        return self.get("Device", "library")

    def get_maxsize(self):
        return int(self.get("General", "tape_max_size"))
