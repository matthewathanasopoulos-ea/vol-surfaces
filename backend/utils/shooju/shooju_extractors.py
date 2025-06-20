from .shooju_tools import ShoojuTools

class ShoojuExtractor:
    def __init__(self):
        self.shooju_helper = ShoojuTools()

    def get_series(self, sid):
        series = self.shooju_helper.sj.get_df(query=sid, fields=None, max_points=-1)
        return series

if __name__ == "__main__":
    connector = ShoojuExtractor()
    print(f"Shooju Server: {connector.shooju_helper.shooju_api_server}")
    print(f"Shooju User Name: {connector.shooju_helper.shooju_api_user_name}")
    print(f"Shooju API Key: {connector.shooju_helper.shooju_api_key}")
