from configparser import ConfigParser

def read_config_file(config_file_path = 'config.ini', section = 'mysql'):
    config = ConfigParser()
    config.read(config_file_path)
    data = {}

    if config.has_section(section):
        params = config.items(section)
        for param in params:
            data[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, config_file_path))
    
    return data

if __name__ == '__main__':
    print(read_config_file())

