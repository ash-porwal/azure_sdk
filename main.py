import configparser



class Main:
    def trigger(self):
        pass


config = configparser.ConfigParser()
config.read("config.ini")

get_conn_str = config['connection_string']['your_conn_string']
print(get_conn_str)

