import json
import random
from scraping.config.config_reader import ConfigReader
from scraping.config.model import ScrapingConfig
from scraping.coordinator import ScraperCoordinator
from scraping.provider import ScraperProvider
from storage.miner.sqlite_miner_storage import SqliteMinerStorage


def generate_scraping_config(
        sub_reddits: list[str],
        cascade_seconds: int = 30,
        max_entities: int = 100,
        num_steps: int = 5
):
    labels_per_steps = len(sub_reddits) // num_steps
    if labels_per_steps == 0:
        labels_to_scrape = [{
            "label_choices": sub_reddits,
            "max_data_entities": max_entities
        }]
    else:
        labels_to_scrape = [{
            "label_choices": sub_reddits[i:i+labels_per_steps],
            "max_data_entities": max_entities
        } for i in range(num_steps)]

    return {
        "scraper_configs": [
            {
                "scraper_id": "Reddit.custom",
                "cadence_seconds": cascade_seconds,
                "labels_to_scrape": labels_to_scrape
            }
        ]

    }


def load_keys():
    with open("keys.json", "r") as f:
        keys = json.load(f)
    return keys


def load_sub_reddits():
    with open("sub_reddits.json", "r") as f:
        sub_reddits = json.load(f)

    return sub_reddits


if __name__ == '__main__':
    database_name = "SqliteMinerStorage.sqlite"
    max_database_size_gb_hint = 250
    print(f"Running custom_crawler.py")

    storage = SqliteMinerStorage(
        database_name,
        max_database_size_gb_hint,
    )
    KEYS = load_keys()
    SUB_REDDITS = load_sub_reddits()
    batch = []
    REDDITS_PER_KEY = len(SUB_REDDITS) // len(KEYS)
    if REDDITS_PER_KEY > 0:
        for i in range(len(KEYS)):
            print(f"Starting scraping coordinator for key {i}")
            num_steps = random.randint(5, 15)
            scraping_config = generate_scraping_config(
                sub_reddits=SUB_REDDITS[i: i + REDDITS_PER_KEY],
                cascade_seconds=random.randint(30, 120),
                max_entities=random.randint(100, 1000),
                num_steps=num_steps
            )
            print(f"Scraping config: {scraping_config}")
            coordinator_config = ScrapingConfig.parse_obj(scraping_config).to_coordinator_config()
            scraping_coordinator = ScraperCoordinator(
                scraper_provider=ScraperProvider(
                    reddit_config=KEYS[i]
                ),
                miner_storage=storage,
                config=coordinator_config
            )
            scraping_coordinator.run_in_background_thread()
            print(f"Started scraping coordinator for key {i}")
    else:
        scraping_config = generate_scraping_config(
            sub_reddits=SUB_REDDITS,
            cascade_seconds=random.randint(30, 120),
            max_entities=random.randint(100, 1000),
            num_steps=random.randint(5, 15)
        )
        print(f"Scraping config: {scraping_config}")
        coordinator_config = ScrapingConfig.parse_obj(scraping_config).to_coordinator_config()
        scraping_coordinator = ScraperCoordinator(
            scraper_provider=ScraperProvider(
                reddit_config=KEYS[0]
            ),
            miner_storage=storage,
            config=coordinator_config
        )
        scraping_coordinator.run_in_background_thread()
        print(f"Started scraping coordinator for key {i}")
    while True:
        import time
        time.sleep(10)
        print(f"Scrapinsg running...")
