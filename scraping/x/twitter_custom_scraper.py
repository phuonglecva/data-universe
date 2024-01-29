import asyncio
import traceback
import bittensor as bt
from typing import List, Optional
from common.data import DataEntity, DataLabel, DataSource
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.x.model import XContent
from scraping.x.utils import is_valid_twitter_url, get_user_from_twitter_url
import datetime as dt
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


class TwitterCustomScraper(Scraper):
    """
    Scrapes tweets using Playwright.
    """

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URI."""
        if not entities:
            return []

        # Treat the entities as guilty until proven innocent.
        results = []

        # Playwright does not support searching for multiple tweet_urls at once. So we must perform each run separately.
        for entity in entities:
            # First check the URI is a valid Twitter URL.
            if not is_valid_twitter_url(entity.uri):
                results.append(
                    ValidationResult(is_valid=False, reason="Invalid URI."),
                    content_size_bytes_validated=entity.content_size_bytes,
                )
                continue

            html = None
            try:
                async with async_playwright() as playwright:
                    chromium = playwright.chromium
                    browser = await chromium.launch()
                    # Consider a user agent.
                    page = await browser.new_page()
                    await page.goto(entity.uri)
                    await page.get_by_test_id("tweet").click(timeout=5000)
                    html = await page.get_by_test_id("tweet").first.inner_html()
                    await browser.close()
            except Exception as e:
                bt.logging.error(
                    f"Failed to validate entity: {traceback.format_exc()}."
                )
                # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                # one caused by malicious input. In my own testing I was able to make this timeout by
                # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                # pass malicious input for chunks they don't have.
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Failed to get Tweet. This can happen if the URI is invalid, or playwright is having an issue.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # Parse the response
            tweet = self._best_effort_parse_tweet_from_html(html, entity.uri)
            if tweet is None:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Tweet not found or is invalid.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # We found the tweet. Validate it.
            results.append(TwitterCustomScraper._validate_tweet(tweet, entity))

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        raise NotImplementedError(
            "Twitter custom scraper only supports validating at this time."
        )

    def _best_effort_parse_tweet_from_html(
        self, html: str, url: str
    ) -> Optional[XContent]:
        """Performs a best effort parsing of a tweets html into XContent"""

        tweet: XContent = None

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Get the username.
            username = get_user_from_twitter_url(url)
            if username is None:
                bt.logging.warning(
                    f"Failed to parse the user from the twitter url: {url}."
                )
                return None

            # Get the text.
            tweet_text_element = soup.find("div", attrs={"data-testid": "tweetText"})
            # tweet_text = tweet_text_element.get_text()

            tweet_text = ""

            # Only find span or images to avoid catching anchor tags and double printing hashtags.
            # TODO: consider if there is a more general approach with NavigableString check.
            for element in tweet_text_element.find_all(
                lambda tag: tag.name in ["span", "img"]
            ):
                # If this is an emoji with an alt text then include this in the text.
                if element.has_attr("alt"):
                    tweet_text += element["alt"]
                # Text defaults to empty string if not existing.
                tweet_text += element.text

            # Get the url.
            url = url

            # Get the timestamp.
            time_element = soup.find("time")
            # Get the datetime attribute from the element and convert to the appropriate format.
            # It is already in utc but we need to add the utc timezone to match exactly.
            timestamp = (
                dt.datetime.strptime(time_element["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
                .replace(second=0)
                .replace(microsecond=0)
                .replace(tzinfo=dt.timezone.utc)
            )

            # TODO: Check for other kinds of tags?
            # Get Hashtags + Cashtags together to keep them in order.
            hashtags = [
                tag.text
                for tag in soup.find_all(
                    "a", {"href": lambda x: x and "ashtag_click" in x}
                )
            ]

            # Cashtags use $ instead of # so ensure first character is #.
            corrected_hashtags = ["#" + hashtag[1:] for hashtag in hashtags]

            tweet = XContent(
                username=username,
                text=tweet_text,
                url=url,
                timestamp=timestamp,
                tweet_hashtags=corrected_hashtags,
            )
        except Exception:
            bt.logging.warning(
                f"Failed to decode XContent from twitter html response: {traceback.format_exc()}."
            )

        return tweet

    # TODO: break this out to utils, or just remove the other scraper.py?
    @classmethod
    def _validate_tweet(cls, tweet: XContent, entity: DataEntity) -> ValidationResult:
        """Validates the tweet is valid by the definition provided by entity."""
        tweet_to_verify = None
        try:
            tweet_to_verify = XContent.from_data_entity(entity)
        except Exception:
            bt.logging.error(
                f"Failed to decode XContent from data entity bytes: {traceback.format_exc()}."
            )
            return ValidationResult(
                is_valid=False,
                reason="Failed to decode data entity",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Previous scrapers would not get the end of longer tweets, replacing with ellipses.
        if (
            tweet.text != tweet_to_verify.text
            and tweet_to_verify.text.endswith("…")
            and tweet_to_verify.text[:-1] in tweet.text
        ):
            bt.logging.trace(
                "Tweet texts match except for one being elided. Using shorter text."
            )
            tweet.text = tweet_to_verify.text

        if tweet_to_verify != tweet:
            bt.logging.info(f"Tweets do not match: {tweet_to_verify} != {tweet}.")
            return ValidationResult(
                is_valid=False,
                reason="Tweet does not match",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Wahey! A valid Tweet.
        # One final check. Does the tweet content match the data entity information?
        try:
            tweet_entity = XContent.to_data_entity(tweet)
            if not DataEntity.are_non_content_fields_equal(tweet_entity, entity):
                return ValidationResult(
                    is_valid=False,
                    reason="The DataEntity fields are incorrect based on the tweet.",
                    content_size_bytes_validated=entity.content_size_bytes,
                )
        except Exception:
            # This shouldn't really happen, but let's safeguard against it anyway to avoid us somehow accepting
            # corrupted or malformed data.
            bt.logging.error(
                f"Failed to convert XContent to DataEntity: {traceback.format_exc()}"
            )
            return ValidationResult(
                is_valid=False,
                reason="Failed to convert XContent to DataEntity.",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # At last, all checks have passed. The DataEntity is indeed valid. Nice work!
        return ValidationResult(
            is_valid=True,
            reason="Good job, you honest miner!",
            content_size_bytes_validated=entity.content_size_bytes,
        )


async def test_validate():
    scraper = TwitterCustomScraper()

    true_entities = [
        DataEntity(
            uri="https://twitter.com/TcMMTsTc/status/1733441357090545731",
            datetime=dt.datetime(2023, 12, 9, 10, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            content=b'{"username":"@TcMMTsTc","text":"\xe3\x81\xbc\xe3\x81\x8f\xe7\x9c\xa0\xe3\x81\x84\xe3\x81\xa7\xe3\x81\x99","url":"https://twitter.com/TcMMTsTc/status/1733441357090545731","timestamp":"2023-12-09T10:59:00Z","tweet_hashtags":[]}',
            content_size_bytes=218,
        ),
        DataEntity(
            uri="https://twitter.com/mdniy/status/1743249601925185642",
            datetime=dt.datetime(2024, 1, 5, 12, 34, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=None,
            content='{"username":"@mdniy","text":"🗓January 6, 2024\\n0️⃣8️⃣ Days to Makar Sankranti 2024\\n📍Sun Temple, Surya Pahar, Goalpura, Assam\\n \\nDepartment of Yogic Science and Naturopathy, Mahapurusha Srimanta Sankaradeva Viswavidyalaya, Assam in collaboration with MDNIY is organizing mass Surya Namaskar Demonstration…","url":"https://twitter.com/mdniy/status/1743249601925185642","timestamp":"2024-01-05T12:34:00Z","tweet_hashtags":[]}',
            content_size_bytes=485,
        ),
        DataEntity(
            uri="https://twitter.com/rEQjoewd6WfNFL3/status/1743187684422799519",
            datetime=dt.datetime(2024, 1, 5, 8, 28, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=None,
            content='{"username":"@rEQjoewd6WfNFL3","text":"ありがとうございます\\n\\nそうなんです\\nほんと偶然です\\n聞いたときはビックリしました\\n\\nいえいえ、私の記念日だなんて\\nもったいないです\\n妹の記念日にしてください\\nぷぷっ","url":"https://twitter.com/rEQjoewd6WfNFL3/status/1743187684422799519","timestamp":"2024-01-05T08:28:00Z","tweet_hashtags":[]}',
            content_size_bytes=253,
        ),
        DataEntity(
            uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
            datetime=dt.datetime(2023, 12, 9, 10, 52, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bittensor"),
            content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            content_size_bytes=484,
        ),
    ]

    results = await scraper.validate(entities=true_entities)
    print(f"Validation results: {results}")

    # Now modify the entities to make them invalid and check validation fails.
    good_entity = true_entities[3]
    bad_entities = [
        good_entity.copy(
            update={"uri": "https://twitter.com/nirmaljajra2/status/abc123"}
        ),
        good_entity.copy(
            update={
                "content": b'{"username":"@nirmaljajra2","text":"Random-text-insertion-DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            }
        ),
        good_entity.copy(
            update={"datetime": good_entity.datetime + dt.timedelta(seconds=1)}
        ),
        # Hashtag ordering needs to be deterministic. Verify changing the order of the hashtags makes the content non-equivalent.
        good_entity.copy(update={"label": DataLabel(value="#PAAl")}),
    ]

    for entity in bad_entities:
        results = await scraper.validate(entities=[entity])
        print(f"Expecting a failed validation. Result={results}")


if __name__ == "__main__":
    asyncio.run(test_validate())