import threading
from typing import Callable, Dict
from common.data import DataSource
from scraping.reddit.reddit_lite_scraper import RedditLiteScraper
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper
from scraping.scraper import Scraper, ScraperId
from scraping.x.microworlds_scraper import MicroworldsTwitterScraper

DEFAULT_FACTORIES = {
    ScraperId.REDDIT_LITE: RedditLiteScraper,
    # For backwards compatibility with old configs, remap x.flash to x.microworlds.
    ScraperId.X_FLASH: MicroworldsTwitterScraper,
    ScraperId.REDDIT_CUSTOM: RedditCustomScraper,
    ScraperId.X_MICROWORLDS: MicroworldsTwitterScraper,
}


class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def __init__(
        self, 
        factories: Dict[DataSource, Callable[[], Scraper]] = DEFAULT_FACTORIES,
        reddit_config: dict = None,
    ):
        self.factories = factories
        self.reddit_config = reddit_config

    def get(self, scraper_id: ScraperId) -> Scraper:
        """Returns a scraper for the given scraper id."""

        assert scraper_id in self.factories, f"Scraper id {scraper_id} not supported."
        if scraper_id == ScraperId.REDDIT_CUSTOM and self.reddit_config:
            return self.factories[scraper_id](
                client_id=self.reddit_config["client_id"],
                client_secret=self.reddit_config["client_secret"],
                username=self.reddit_config["username"],
                password=self.reddit_config["password"],
            )
        return self.factories[scraper_id]()
