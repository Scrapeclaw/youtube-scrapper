# 🎥 YouTube Channel Scraper

> Part of **[ScrapeClaw](https://www.scrapeclaw.cc/)** — a suite of production-ready, agentic social media scrapers for Instagram, YouTube, X/Twitter, and Facebook. Built with Python & Playwright. No API keys required.

[![ScrapeClaw](https://img.shields.io/badge/ScrapeClaw-Visit%20Site-blue?style=flat-square)](https://www.scrapeclaw.cc/)
[![ClawHub](https://img.shields.io/badge/ClawHub-View%20Skill-green?style=flat-square)](https://clawhub.ai/ArulmozhiV/youtube-scrapper)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-PayPal-yellow?style=flat-square&logo=paypal)](https://www.paypal.com/paypalme/arulmozhivelu)

---

## What Is This?

A browser-based YouTube scraper that discovers and extracts structured data from **public YouTube channels** — without any official API. It uses Playwright for full browser automation with built-in anti-detection, fingerprinting, and human behavior simulation to scrape at scale reliably.

Two-phase workflow:
1. **Discovery** — Find YouTube channels by location and category via Google Search
2. **Scraping** — Extract full channel data, subscriber counts, videos, and metadata using a real browser session

---

## Features

| Feature | Description |
|---------|-------------|
| 🔍 **Discovery** | Find channels by city and category automatically |
| 🌐 **Browser Simulation** | Full Playwright browser — renders JavaScript, bypasses bot detection |
| 🛡️ **Anti-Detection** | Browser fingerprinting, stealth scripts, human behavior simulation |
| 📊 **Rich Data** | Channel info, subscriber counts, video metrics, influencer tier |
| 🎬 **Video Data** | Recent videos with title, views, upload date, duration, and thumbnail URL |
| 🖼️ **Media Download** | Profile pics and video thumbnails saved to Key-Value store |
| 💾 **Flexible Export** | JSON and CSV output formats |
| 🔄 **Resume Support** | Checkpoint-based resume for interrupted sessions |
| ⚡ **Smart Filtering** | Auto-skip channels below subscriber threshold |
| 🌍 **Residential Proxy** | Built-in proxy manager supporting 4 major providers |

---

## Installation

```bash
# Clone the repository
git clone https://github.com/Scrapeclaw/youtube-scrapper.git
cd youtube-scrapper

# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### Environment Setup

Create a `.env` file in the project root:

```env
# Google Custom Search API (optional, for discovery)
GOOGLE_API_KEY=your_google_api_key
GOOGLE_SEARCH_ENGINE_ID=your_search_engine_id

# Residential proxy (optional — see Proxy section below)
PROXY_ENABLED=false
PROXY_PROVIDER=brightdata
PROXY_USERNAME=your_proxy_user
PROXY_PASSWORD=your_proxy_pass
PROXY_COUNTRY=in
PROXY_STICKY=true
```

---

## Usage

### Discover Channels

```bash
# Discover tech channels in India
python scripts/youtube_orchestrator.py --config resources/scraper_config_ind.json --discovery-only

# Discover gaming channels in the US
python scripts/youtube_orchestrator.py --config resources/scraper_config_us.json --discovery-only

# Discover with custom categories and locations
python scripts/youtube_orchestrator.py --config resources/scraper_config_ind.json --categories tech gaming --locations "Mumbai" "Delhi"
```

### Scrape

```bash
# Full pipeline — discover then scrape (India preset)
python scripts/youtube_orchestrator.py --config resources/scraper_config_ind.json

# Scrape from an existing discovery queue file
python scripts/youtube_orchestrator.py --scrape-only --queue data/queue/ind/mixed_India_20260226.json

# Resume an interrupted scrape session
python scripts/youtube_orchestrator.py --config resources/scraper_config_ind.json --resume

# Run with browser window visible (for debugging)
python scripts/youtube_orchestrator.py --config resources/scraper_config_ind.json --no-headless
```

### Run as Apify Actor

```bash
# Install Apify SDK
pip install apify-sdk

# Run the actor entry point locally
python src/main.py
```

---

## Output Data

Each scraped channel is saved as one record in the default dataset:

```json
{
  "channel_id": "TechBurner",
  "channel_name": "Tech Burner",
  "handle": "@TechBurner",
  "channel_url": "https://www.youtube.com/@TechBurner",
  "subscribers": 8500000,
  "influencer_tier": "mega",
  "video_count": 6,
  "total_views": 950000000,
  "description": "India's largest tech YouTube channel…",
  "profile_pic_url": "https://yt3.googleusercontent.com/…",
  "banner_url": "https://yt3.googleusercontent.com/…",
  "is_verified": true,
  "joined_date": "Jan 1, 2016",
  "country": "India",
  "category": "tech",
  "location": "India",
  "external_links": ["https://instagram.com/techburner"],
  "recent_videos": [
    {
      "video_id": "dQw4w9WgXcQ",
      "title": "Best Budget Smartphones 2025",
      "thumbnail_url": "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
      "view_count": 1200000,
      "upload_date": "2 days ago",
      "duration": "12:34"
    }
  ],
  "video_urls": ["https://www.youtube.com/watch?v=dQw4w9WgXcQ"],
  "scrape_timestamp": "2026-03-03T10:30:00.000Z"
}
```

### Influencer Tiers

| Tier | Subscribers |
|------|-------------|
| nano | < 10,000 |
| micro | 10,000 – 99,999 |
| mid | 100,000 – 999,999 |
| macro | 1,000,000 – 9,999,999 |
| mega | > 10,000,000 |

---



## Configuration Reference

Edit `resources/scraper_config_ind.json` to customise behaviour:

```json
{
  "proxy": {
    "enabled": false,
    "provider": "brightdata",
    "country": "in",
    "sticky": true,
    "sticky_ttl_minutes": 10
  },
  "categories": ["gaming", "tech", "beauty", "fashion", "fitness", "food"],
  "locations": ["India", "Mumbai", "Delhi", "Bangalore"],
  "max_videos_to_scrape": 6,
  "headless": false,
  "results_per_search": 20,
  "search_delay": [3, 7],
  "scrape_delay": [2, 5],
  "rate_limit_wait": 60,
  "max_retries": 3
}
```

---

## Project Structure

```
youtube-scrapper/
├── src/
│   └── main.py                        # Apify actor entry point
├── scripts/
│   ├── youtube_orchestrator.py        # Resilient orchestration pipeline
│   ├── youtube_channel_discovery.py   # Google Search-based channel discovery
│   ├── youtube_channel_scraper.py     # Playwright-based channel scraper
│   ├── anti_detection.py              # Fingerprinting & stealth helpers
│   └── proxy_manager.py               # Residential proxy integration
├── resources/
│   ├── scraper_config_ind.json        # India region preset
│   ├── scraper_config_us.json         # US region preset
│   ├── scraper_config_uk.json         # UK region preset
│   └── scraper_config_*.json          # Other region presets
├── .actor/
│   ├── actor.json                     # Apify actor manifest
│   ├── input_schema.json              # Input schema (UI form)
│   └── output_schema.json             # Output schema
├── Dockerfile
└── requirements.txt
```

---

## Part of ScrapeClaw

This scraper is one of several tools in the **[ScrapeClaw](https://www.scrapeclaw.cc/)** collection:

| Scraper | Description | Links |
|---------|-------------|-------|
| 🎥 **YouTube** | Channels, subscribers & video metadata | [GitHub](https://github.com/Scrapeclaw/youtube-scrapper) · [ClawHub](https://clawhub.ai/ArulmozhiV/youtube-scrapper) |
| 📸 **Instagram** | Profiles, posts, media & follower counts | [GitHub](https://github.com/Scrapeclaw/instagram-scraper) · [ClawHub](https://clawhub.ai/ArulmozhiV/instagram-scraper) |
| 📘 **Facebook** | Pages, groups, posts & engagement data | [GitHub](https://github.com/Scrapeclaw/facebook-scraper) · [ClawHub](https://clawhub.ai/ArulmozhiV/facebook-scraper) |
| 🐦 **X / Twitter** | Tweets, profiles & engagement metrics | [GitHub](https://github.com/Scrapeclaw/twitter-scraper) · [ClawHub](https://clawhub.ai/ArulmozhiV/x-twitter-scraper) |

All scrapers share the same anti-detection foundation, proxy support, and JSON/CSV export pipeline.

---

## 🚀 ScrapeClaw Customised Solutions

> **We build, you own.** No per-credit fees. Stop renting data — own your entire scraping infrastructure.

ScrapeClaw offers two commercial offerings for teams and businesses that need more than open-source:

### 📦 Tailored Datasets

Get pre-scraped or on-demand datasets built around your exact industry, platform, or niche — delivered ready for analysis.

- Industry-specific social media datasets on demand
- Custom extraction logic ("Skills") for your use case
- One-time delivery or recurring data feeds
- Output in CSV, JSON, or direct database delivery

👉 [**Request a Dataset →**](https://www.scrapeclaw.cc/)

### 🏗️ Private Infrastructure Setup ★ High Value

We deploy a turnkey ScrapeClaw system on your own servers — you own 100% of the infrastructure and the data.

- 🔒 **Privacy & Compliance** — data never leaves your network, ideal for FinTech & Health
- 🤖 **Self-Healing Agents** — AI-powered scrapers that adapt when sites change
- 💸 **Slash API Costs** — stop paying $1–5 per 1K requests; scrape 1M rows at flat infra cost
- Includes **1 month of managed maintenance & support**

👉 [**Book a Strategy Call →**](https://www.scrapeclaw.cc/)

---

## ☕ Support This Project

If this tool saves you time or helps your workflow, consider buying me a coffee — it keeps the project maintained and new scrapers coming!

[![Buy Me a Coffee via PayPal](https://img.shields.io/badge/☕%20Buy%20Me%20a%20Coffee-PayPal-blue?style=for-the-badge&logo=paypal)](https://www.paypal.com/paypalme/arulmozhivelu)

👉 **[paypal.me/arulmozhivelu](https://www.paypal.com/paypalme/arulmozhivelu)**

---

## Disclaimer

This tool is intended for scraping **publicly available** data only. No login is required or used. Always comply with YouTube's Terms of Service and your local data privacy regulations. The author is not responsible for any misuse.

---

*Built by [ScrapeClaw](https://www.scrapeclaw.cc/) · [View all scrapers](https://apify.com/scrapeclaw)*
