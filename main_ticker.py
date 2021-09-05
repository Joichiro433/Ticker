import asyncio

from trading_api.trading_api import ApiClient


if __name__ == '__main__':
    try:
        api_client : ApiClient = ApiClient()
        asyncio.run(api_client.get_realtime_orderbook())
    except KeyboardInterrupt:  # Ctrl+C to break
       pass