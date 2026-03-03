#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apify Actor entry point for YouTube Channel Scraper & Discovery.

This module wraps the existing youtube_channel_discovery and
youtube_channel_scraper scripts so they run inside the Apify platform.

Input  → Actor.get_input()
Output → Actor.push_data()  (default dataset)
Images → Actor.set_value()  (key-value store, optional)
State  → Actor.set_value() / Actor.get_value() (key-value store)
"""

import asyncio
import json
import os
import sys
import logging
import traceback
import tempfile
import random
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from apify import Actor

# ---------------------------------------------------------------------------
# Ensure the scripts/ directory is importable
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Region presets (mirrors the resources/scraper_config_*.json files)
# ---------------------------------------------------------------------------
REGION_PRESETS: Dict[str, Dict] = {
    "us": {
        "categories": ["gaming", "tech", "beauty", "fashion", "fitness", "food",
                        "travel", "music", "education", "comedy", "finance"],
        "locations": ["United States", "New York", "Los Angeles", "Chicago",
                      "Houston", "Phoenix", "San Francisco"],
    },
    "uk": {
        "categories": ["gaming", "tech", "beauty", "fashion", "fitness", "food",
                        "travel", "music", "education", "comedy", "lifestyle"],
        "locations": ["United Kingdom", "London", "Manchester", "Birmingham",
                      "Glasgow", "Leeds", "Liverpool"],
    },
    "ind": {
        "categories": ["gaming", "tech", "beauty", "fashion", "fitness", "food",
                        "travel", "music", "education", "comedy", "lifestyle",
                        "cooking", "diy", "art", "finance", "health", "entertainment"],
        "locations": ["India", "Mumbai", "Delhi", "Bangalore", "Hyderabad",
                      "Chennai", "Kolkata", "Pune", "Ahmedabad", "Jaipur"],
    },
    "eur": {
        "categories": ["gaming", "tech", "beauty", "fashion", "fitness", "food",
                        "travel", "music", "education", "lifestyle"],
        "locations": ["Europe", "Germany", "France", "Spain", "Italy",
                      "Netherlands", "Sweden", "Paris", "Berlin", "Amsterdam"],
    },
    "gulf": {
        "categories": ["gaming", "tech", "beauty", "fashion", "lifestyle", "food",
                        "travel", "music", "education", "finance"],
        "locations": ["UAE", "Dubai", "Abu Dhabi", "Saudi Arabia", "Riyadh",
                      "Kuwait", "Qatar", "Doha", "Bahrain", "Oman"],
    },
    "east": {
        "categories": ["gaming", "tech", "beauty", "fashion", "food", "travel",
                        "music", "education", "anime", "lifestyle"],
        "locations": ["Japan", "South Korea", "China", "Thailand", "Vietnam",
                      "Indonesia", "Philippines", "Singapore", "Malaysia", "Tokyo"],
    },
}


# ---------------------------------------------------------------------------
# Helper: build a temporary config dict from actor input
# ---------------------------------------------------------------------------
def build_config(actor_input: Dict) -> Dict:
    region = actor_input.get("region", "ind").lower()
    preset = REGION_PRESETS.get(region, REGION_PRESETS["ind"])

    categories = actor_input.get("categories") or preset["categories"]
    locations = actor_input.get("locations") or preset["locations"]

    return {
        "proxy": {"enabled": False},  # proxy handled externally by Apify
        "categories": categories,
        "locations": locations,
        "max_videos_to_scrape": actor_input.get("maxVideosPerChannel", 6),
        "headless": actor_input.get("headless", True),
        "results_per_search": 20,
        "search_delay": [3, 7],
        "scrape_delay": [2, 5],
        "rate_limit_wait": 60,
        "max_retries": actor_input.get("maxDiscoveryRetries", 3),
    }


# ---------------------------------------------------------------------------
# Helper: write config to a temp file so existing scripts can read it
# ---------------------------------------------------------------------------
def write_temp_config(config: Dict) -> str:
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    )
    json.dump(config, tmp)
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# Helper: build Playwright proxy kwargs from Apify proxy info
# ---------------------------------------------------------------------------
def build_playwright_proxy(proxy_info: Optional[Dict]) -> Optional[Dict]:
    """
    Convert an Apify proxy info dict (returned by proxy_configuration.new_url())
    into a Playwright-compatible proxy dict.
    """
    if not proxy_info:
        return None
    url = proxy_info if isinstance(proxy_info, str) else proxy_info.get("url")
    if not url:
        return None
    return {"server": url}


# ---------------------------------------------------------------------------
# Apify-aware channel scraper wrapper
# ---------------------------------------------------------------------------
class ApifyChannelScraper:
    """
    Thin wrapper around YouTubeScraperPlaywright that:
    - pushes each scraped record to the Apify dataset
    - optionally stores thumbnail images in KV store
    - respects maxChannels limit
    """

    def __init__(
        self,
        config_path: str,
        proxy_url: Optional[str],
        max_videos: int,
        download_thumbnails: bool,
        min_subscribers: int,
        max_channels: int,
    ):
        self.config_path = config_path
        self.proxy_url = proxy_url
        self.max_videos = max_videos
        self.download_thumbnails = download_thumbnails
        self.min_subscribers = min_subscribers
        self.max_channels = max_channels  # 0 = unlimited
        self._scraped = 0

    async def scrape_channels(self, channels: List[Any]) -> Dict:
        """Scrape a list of channel entries and push results to dataset."""
        from youtube_channel_scraper import (
            YouTubeScraperPlaywright,
            ChannelNotFoundException,
            ChannelSkippedException,
            RateLimitException,
            determine_influencer_tier,
        )

        stats = {"success": 0, "failed": 0, "skipped": 0}

        scraper = YouTubeScraperPlaywright(config_path=self.config_path)

        # Patch the browser startup to inject Apify proxy
        original_start = scraper.start_browser

        async def patched_start(headless=True):
            await original_start(headless=headless)
            if self.proxy_url and scraper.context:
                # Re-create context with proxy (Playwright requires proxy at context creation)
                # We patch at the launch level instead; handled below.
                pass

        # If we have a proxy URL, monkey-patch the proxy manager
        if self.proxy_url:
            from unittest.mock import MagicMock
            mock_pm = MagicMock()
            mock_pm.enabled = True
            mock_pm.get_playwright_proxy.return_value = {"server": self.proxy_url}
            scraper.proxy_manager = mock_pm

        await scraper.start_browser(headless=True)

        try:
            for channel_entry in channels:
                if self.max_channels and self._scraped >= self.max_channels:
                    logger.info(f"Reached maxChannels limit ({self.max_channels}). Stopping.")
                    break

                if isinstance(channel_entry, str):
                    channel_id = channel_entry
                    category = None
                    location = None
                else:
                    channel_id = channel_entry.get("channel_id") or channel_entry.get("handle", "")
                    category = channel_entry.get("category")
                    location = channel_entry.get("location")

                try:
                    channel_data = await scraper.scrape_channel(channel_id, category, location)

                    # Apply subscriber filter
                    subs = channel_data.get("subscribers", 0)
                    if self.min_subscribers and subs < self.min_subscribers:
                        logger.info(
                            f"Skipping {channel_id}: {subs:,} subscribers < minimum {self.min_subscribers:,}"
                        )
                        stats["skipped"] += 1
                        continue

                    # Handle thumbnails
                    if not self.download_thumbnails:
                        # Remove local paths if thumbnails not requested
                        channel_data.pop("profile_pic_local", None)
                        channel_data.pop("banner_local", None)
                        channel_data.pop("video_thumbnails_local", None)
                        for v in channel_data.get("recent_videos", []):
                            v.pop("thumbnail_local", None)
                    else:
                        # Store thumbnails in KV store
                        await self._store_thumbnails(channel_data)

                    # Push to Apify dataset
                    await Actor.push_data(channel_data)
                    logger.info(
                        f"[{self._scraped + 1}] Pushed: {channel_data.get('channel_name', channel_id)} "
                        f"({subs:,} subs, tier={channel_data.get('influencer_tier')})"
                    )

                    self._scraped += 1
                    stats["success"] += 1

                except ChannelNotFoundException:
                    logger.warning(f"Channel not found: {channel_id}")
                    stats["failed"] += 1
                except ChannelSkippedException:
                    stats["skipped"] += 1
                except RateLimitException:
                    logger.warning("Rate limited — sleeping 60 s…")
                    await asyncio.sleep(60)
                    stats["failed"] += 1
                except Exception as exc:
                    logger.error(f"Error scraping {channel_id}: {exc}")
                    stats["failed"] += 1

                # Polite delay
                await asyncio.sleep(random.uniform(2, 5))

        finally:
            await scraper.cleanup()

        return stats

    async def _store_thumbnails(self, channel_data: Dict):
        """Upload downloaded thumbnail files to the Apify Key-Value store."""
        channel_id = channel_data.get("channel_id", "unknown")

        async def _upload(local_path: str, key: str):
            p = Path(local_path)
            if p.exists():
                with open(p, "rb") as fh:
                    data = fh.read()
                await Actor.set_value(key, data, content_type="image/jpeg")
                logger.debug(f"Stored thumbnail: {key}")

        if channel_data.get("profile_pic_local"):
            await _upload(
                channel_data["profile_pic_local"],
                f"thumb_{channel_id}_profile",
            )

        if channel_data.get("banner_local"):
            await _upload(
                channel_data["banner_local"],
                f"thumb_{channel_id}_banner",
            )

        for video in channel_data.get("recent_videos", []):
            if video.get("thumbnail_local") and video.get("video_id"):
                await _upload(
                    video["thumbnail_local"],
                    f"thumb_{channel_id}_video_{video['video_id']}",
                )


# ---------------------------------------------------------------------------
# Discovery wrapper: returns list of channel dicts
# ---------------------------------------------------------------------------
async def run_discovery(config_path: str, config: Dict, proxy_url: Optional[str]) -> List[Dict]:
    """Run YouTubeChannelDiscovery and return discovered channels."""
    from youtube_channel_discovery import YouTubeChannelDiscovery

    discovery = YouTubeChannelDiscovery(config_path=config_path)

    # Inject Apify proxy if available
    if proxy_url and discovery.proxy_manager is not None:
        from unittest.mock import MagicMock
        mock_pm = MagicMock()
        mock_pm.enabled = True
        mock_pm.get_playwright_proxy.return_value = {"server": proxy_url}
        discovery.proxy_manager = mock_pm

    await discovery.start_browser(headless=True)

    try:
        channels = await discovery.discover_channels(
            categories=config.get("categories"),
            locations=config.get("locations"),
            resume=False,
        )
        return channels or []
    finally:
        await discovery.cleanup()


# ---------------------------------------------------------------------------
# Main actor logic
# ---------------------------------------------------------------------------
async def main():
    async with Actor:
        # ----------------------------------------------------------------
        # 1. Read input
        # ----------------------------------------------------------------
        actor_input: Dict = await Actor.get_input() or {}
        logger.info(f"Actor input: {json.dumps(actor_input, indent=2, default=str)}")

        mode = actor_input.get("mode", "full")
        region = actor_input.get("region", "ind").lower()
        min_subscribers = actor_input.get("minSubscribers", 0)
        max_channels = actor_input.get("maxChannels", 100)
        download_thumbnails = actor_input.get("downloadThumbnails", False)
        max_videos = actor_input.get("maxVideosPerChannel", 6)
        channel_handles = actor_input.get("channelHandles", [])

        # ----------------------------------------------------------------
        # 2. Proxy configuration
        # ----------------------------------------------------------------
        proxy_url: Optional[str] = None
        proxy_cfg_input = actor_input.get("proxyConfiguration")
        if proxy_cfg_input:
            try:
                proxy_configuration = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_cfg_input
                )
                if proxy_configuration:
                    proxy_url = await proxy_configuration.new_url()
                    logger.info(f"Using Apify proxy: {proxy_url[:50]}…")
            except Exception as exc:
                logger.warning(f"Could not create proxy configuration: {exc}")

        # ----------------------------------------------------------------
        # 3. Build temporary config file
        # ----------------------------------------------------------------
        config = build_config(actor_input)
        config_path = write_temp_config(config)
        logger.info(f"Wrote temp config to {config_path}")

        # ----------------------------------------------------------------
        # 4. Execute based on mode
        # ----------------------------------------------------------------
        channels_to_scrape: List[Any] = []

        if mode == "scrape_channels":
            # User supplied explicit handles
            if not channel_handles:
                await Actor.fail(status_message="mode=scrape_channels requires channelHandles to be set.")
                return
            channels_to_scrape = channel_handles
            logger.info(f"scrape_channels mode: {len(channels_to_scrape)} handles provided")

        elif mode == "discovery_only":
            logger.info("discovery_only mode: running discovery, pushing handles to dataset…")
            channels = await run_discovery(config_path, config, proxy_url)
            logger.info(f"Discovered {len(channels)} channels")
            for ch in channels:
                await Actor.push_data(ch if isinstance(ch, dict) else {"handle": ch})
            logger.info("Discovery complete. Exiting.")
            return

        else:  # full
            logger.info("full mode: discovering channels then scraping…")

            # Persist state to KV store for resilience
            state_key = f"state_{region}"
            state = await Actor.get_value(state_key) or {}

            if state.get("channels") and state.get("phase") not in ("completed", None):
                logger.info(f"Resuming from saved state ({len(state['channels'])} channels)")
                channels_to_scrape = state["channels"]
            else:
                try:
                    channels = await run_discovery(config_path, config, proxy_url)
                    logger.info(f"Discovery found {len(channels)} channels")
                    channels_to_scrape = channels
                    await Actor.set_value(
                        state_key,
                        {"channels": channels_to_scrape, "phase": "scraping",
                         "discovered_at": datetime.now(timezone.utc).isoformat()},
                    )
                except Exception as exc:
                    logger.error(f"Discovery failed: {exc}")
                    await Actor.fail(status_message=f"Discovery failed: {exc}")
                    return

        # ----------------------------------------------------------------
        # 5. Scrape channels
        # ----------------------------------------------------------------
        if not channels_to_scrape:
            logger.warning("No channels to scrape. Finishing.")
            return

        logger.info(f"Starting to scrape {len(channels_to_scrape)} channels (max={max_channels or 'unlimited'})…")

        scraper_wrapper = ApifyChannelScraper(
            config_path=config_path,
            proxy_url=proxy_url,
            max_videos=max_videos,
            download_thumbnails=download_thumbnails,
            min_subscribers=min_subscribers,
            max_channels=max_channels,
        )

        stats = await scraper_wrapper.scrape_channels(channels_to_scrape)

        logger.info(
            f"Scraping complete — success={stats['success']}, "
            f"failed={stats['failed']}, skipped={stats['skipped']}"
        )

        # Mark state as completed
        if mode == "full":
            await Actor.set_value(
                f"state_{region}",
                {"phase": "completed",
                 "completed_at": datetime.now(timezone.utc).isoformat(),
                 "stats": stats},
            )

        # ----------------------------------------------------------------
        # 6. Clean up temp file
        # ----------------------------------------------------------------
        try:
            Path(config_path).unlink(missing_ok=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
