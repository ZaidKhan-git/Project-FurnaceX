# B2B Sales Intelligence Platform - Petroleum Intel

A policy-safe web scraping system for identifying early demand signals for petroleum products.

## Quick Start

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run complete intelligence pipeline (Scrape -> Enrich -> Score -> Map)
python run_full_pipeline.py

# Start dashboard
python main.py serve
```

## Features

- 🔍 **Multi-source Intelligence**: PARIVESH, CPPP, GeM, NHAI, BSE/NSE
- 🎯 **Smart Lead Scoring**: Ranks leads by signal strength & recency
- 🛡️ **Policy-Safe**: robots.txt compliance, rate limiting, honest User-Agent
- 📊 **Premium Dashboard**: Dark mode, filters, CSV export

## Project Structure

```
petroleum-intel/
├── config/settings.py       # Configuration
├── scrapers/                # Data source scrapers
├── models/                  # Database & data models
├── intelligence/            # Lead scoring & mapping
├── api/                     # Flask backend
├── static/                  # CSS, JS
├── templates/               # HTML templates
└── tests/                   # Unit tests
```

## License

MIT - Built for Productathon 2026
