import configparser


def load_config(path):
    cfg = configparser.ConfigParser()
    cfg.read(path)

    # only use config file if "ploomber section"
    if "ploomber" in cfg:
        return cfg

    return None
