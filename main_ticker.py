import asyncio

from trading_api.trading_api import ApiClient
from logger import Logger

logger = Logger()
logger.remove_oldlog()


if __name__ == '__main__':
    try:
        logger.info('####### START #######')
        api_client : ApiClient = ApiClient()
        asyncio.run(api_client.get_realtime_orderbook())
        
    except KeyboardInterrupt:  # Ctrl+C to break
        pass

    except Exception as e:
        logger.error(e)
        
    finally:
        api_client.save_ticker()
        logger.info('######## END ########')