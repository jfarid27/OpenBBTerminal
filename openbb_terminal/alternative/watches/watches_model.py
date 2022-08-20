"""Classes for watch data fetching."""

from abc import ABCMeta, abstractmethod
import logging
import pandas as pd
import requests

from openbb_terminal.decorators import log_start_end

logger = logging.getLogger(__name__)


class WatchDataAPI(metaclass=ABCMeta):
    """Abstract class for watch data API."""

    @abstractmethod
    def get_data(self):
        """Get watch data."""
        pass


class WatchChartsAPI(WatchDataAPI):
    """
    The WatchChartsAPI class.
    """

    APIs = {
        "market_index": "https://watchcharts.com/watches/brand_chart.json?&_={0}",
        "brand": "https://watchcharts.com/watches/brand_chart/{0}.json?&_={1}",
    }
    BRAND_CODES = {
        "rolex": "24",
        "patek_philippe": "219",
        "audemars_piguet": "50",
        "vacheron_constantin": "220",
        "hublot": "259",
        "omega": "12",
        "tag_heuer": "34",
        "seiko": "3",
        "cartier": "52",
    }

    @log_start_end(log=logger)
    def get_data_by_code(self, current_time=pd.Timestamp.now(), brand=None):
        """
        Get the watch charts index data for a given brand code and return a dataframe of chartpoints.

        Keyword arguments:
        current_time -- the current time to use for the request (default pd.Timestamp.now())
        brand -- the brand code to use for the request. (default None)
        """
        current_time_milli = int(current_time.timestamp() * 1000)
        logging.info(
            "No brand specified. Fetching market index data."
        ) if not brand else None
        url = (
            self.APIs["brand"].format(brand, current_time_milli)
            if brand
            else self.APIs["market_index"].format(current_time_milli)
        )
        res = requests.get(url.format(brand, current_time_milli))
        if res.status_code != 200:
            logging.error(f"Failed to get watch charts index data for brand {brand}")
            return pd.DataFrame()
        data = res.json()
        df = pd.DataFrame(data)
        return df

    @log_start_end(log=logger)
    def get_data(self, brand=None, **kwargs):
        """
        Get the watch charts index data for a given brand name and return a dataframe of chartpoints.

        Keyword arguments:
        current_time -- the current time to use for the request (default pd.Timestamp.now())
        brand -- the brand name to use for the request. See BRAND_CODES (default None)
        """
        code = self.BRAND_CODES.get(brand)
        logging.warn(f"No brand code found for brand {brand}") if not code else None
        return self.get_data_by_code(brand=code, **kwargs)
