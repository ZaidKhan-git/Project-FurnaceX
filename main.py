"""
Petroleum Intel - CLI Entry Point
Main command-line interface for running scrapers and starting the server.
"""

import sys
import logging
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from config.settings import LOG_LEVEL, LOG_FORMAT
from models.database import get_db
from scrapers import SCRAPER_REGISTRY
from intelligence.scorer import get_scorer
from intelligence.mapper import get_mapper

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

console = Console()


@click.group()
@click.version_option(version="1.0.0", prog_name="Petroleum Intel")
def cli():
    """🛢️ Petroleum Intel - B2B Sales Intelligence Platform
    
    Policy-safe web scraping for petroleum product demand signals.
    """
    pass


@cli.command()
@click.option('--source', '-s', type=click.Choice(['all'] + list(SCRAPER_REGISTRY.keys())), 
              default='all', help='Data source to scrape')
@click.option('--dry-run', is_flag=True, help='Run without saving to database')
def scrape(source: str, dry_run: bool):
    """🔍 Run scrapers to collect new leads."""
    
    # db = get_db()
    # pipeline = IntelligencePipeline()
    
    from intelligence.pipeline import IntelligencePipeline
    pipeline = IntelligencePipeline()
    db = get_db()
    
    sources = list(SCRAPER_REGISTRY.keys()) if source == 'all' else [source]
    
    total_leads = 0
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        
        for src in sources:
            task = progress.add_task(f"Scraping {src}...", total=None)
            
            try:
                # Create and run scraper
                scraper_factory = SCRAPER_REGISTRY[src]
                scraper = scraper_factory()
                
                signals = scraper.run()
                
                if signals:
                    processed_signals = [pipeline.process_lead(s) for s in signals]
                    
                    if not dry_run:
                        result = db.insert_leads_batch(processed_signals)
                        console.print(
                            f"  ✅ {src}: {result['inserted']} new, "
                            f"{result['duplicates']} duplicates"
                        )
                        total_leads += result['inserted']
                    else:
                        console.print(f"  🔍 {src}: {len(signals)} signals (dry run)")
                        total_leads += len(signals)
                else:
                    console.print(f"  ⚠️ {src}: No signals found")
                    
            except Exception as e:
                console.print(f"  ❌ {src}: {e}", style="red")
                logger.error(f"Scraper {src} failed: {e}", exc_info=True)
            
            progress.remove_task(task)
    
    console.print(f"\n🎯 Total: {total_leads} leads collected", style="bold green")


@cli.command()
@click.option('--limit', '-n', default=20, help='Number of leads to show')
@click.option('--product', '-p', default=None, help='Filter by product')
@click.option('--min-score', '-m', default=None, type=float, help='Minimum score')
def leads(limit: int, product: str, min_score: float):
    """📋 View collected leads."""
    
    db = get_db()
    leads_data = db.get_leads(product=product, min_score=min_score, limit=limit)
    
    if not leads_data:
        console.print("No leads found.", style="yellow")
        return
    
    table = Table(title=f"🛢️ Top {len(leads_data)} Leads")
    
    table.add_column("Score", justify="center", style="cyan")
    table.add_column("Company", style="white", max_width=30)
    table.add_column("Signal", style="yellow")
    table.add_column("Product", style="green")
    table.add_column("Sector", style="magenta")
    table.add_column("Source", style="blue")
    
    for lead in leads_data:
        score = lead.get('score', 0)
        
        # Color-code score
        if score >= 80:
            score_str = f"[bold red]{score:.0f}[/]"
        elif score >= 60:
            score_str = f"[orange1]{score:.0f}[/]"
        else:
            score_str = f"{score:.0f}"
        
        table.add_row(
            score_str,
            lead.get('company_name', 'Unknown')[:30],
            lead.get('signal_type', ''),
            lead.get('product_match', '-')[:15] if lead.get('product_match') else '-',
            lead.get('sector', '-'),
            lead.get('source', ''),
        )
    
    console.print(table)


@cli.command()
def stats():
    """📊 Show dashboard statistics."""
    
    db = get_db()
    stats_data = db.get_stats()
    
    console.print("\n📊 [bold]Petroleum Intel Dashboard[/bold]\n")
    
    console.print(f"  Total Leads: [bold cyan]{stats_data.get('total_leads', 0)}[/]")
    console.print(f"  High-Score (70+): [bold red]{stats_data.get('high_score_leads', 0)}[/]")
    console.print(f"  Last 24h: [bold green]{stats_data.get('recent_leads', 0)}[/]")
    
    # By Status
    by_status = stats_data.get('by_status', {})
    if by_status:
        console.print("\n  [bold]By Status:[/]")
        for status, count in by_status.items():
            console.print(f"    {status}: {count}")
    
    # By Product
    by_product = stats_data.get('by_product', {})
    if by_product:
        console.print("\n  [bold]By Product:[/]")
        for product, count in sorted(by_product.items(), key=lambda x: x[1], reverse=True)[:5]:
            console.print(f"    {product}: {count}")


@cli.command()
@click.option('--host', '-h', default='127.0.0.1', help='Host to bind to')
@click.option('--port', '-p', default=5000, help='Port to bind to')
@click.option('--debug', is_flag=True, help='Enable debug mode')
def serve(host: str, port: int, debug: bool):
    """🌐 Start the web dashboard server."""
    
    console.print(f"\n🌐 Starting Petroleum Intel Dashboard...")
    console.print(f"   URL: http://{host}:{port}")
    console.print(f"   Press Ctrl+C to stop\n")
    
    from api.app import create_app
    app = create_app()
    app.run(host=host, port=port, debug=debug)


@cli.command()
@click.option('--format', '-f', type=click.Choice(['csv', 'json']), default='csv')
@click.option('--output', '-o', default='leads_export', help='Output filename (without extension)')
@click.option('--min-confidence', default=30.0, help='Minimum confidence score filter')
def export(format: str, output: str, min_confidence: float):
    """💾 Export leads to file."""
    import json
    import csv
    
    db = get_db()
    leads_data = db.get_leads(limit=10000)  # Export all
    
    # Filter by confidence
    original_count = len(leads_data)
    leads_data = [l for l in leads_data if l.get("confidence", 0) >= min_confidence]
    
    if len(leads_data) < original_count:
        console.print(f"Filtered {original_count - len(leads_data)} low-confidence leads (<{min_confidence}%)", style="dim")
    
    if not leads_data:
        console.print("No leads to export.", style="yellow")
        return
    
    output_path = PROJECT_ROOT / "data" / f"{output}.{format}"
    output_path.parent.mkdir(exist_ok=True)
    
    if format == 'csv':
        # Add human-readable keywords summary
        for lead in leads_data:
            keywords_dict = lead.get("keywords_matched", {})
            if isinstance(keywords_dict, str):
                import json
                try:
                    keywords_dict = json.loads(keywords_dict)
                except:
                    keywords_dict = {}
            
            # Create readable summary: "Boiler, DG Set [machinery] | Bitumen, VG-30 [commodities]"
            summary_parts = []
            for category, kw_list in keywords_dict.items():
                if kw_list:
                    kw_str = ", ".join(kw_list[:3])  # Limit to 3 per category
                    summary_parts.append(f"{kw_str} [{category}]")
            
            lead["keywords_summary"] = " | ".join(summary_parts) if summary_parts else "No keywords"
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            if leads_data:
                # Reorder columns to put keywords_summary after confidence
                all_keys = list(leads_data[0].keys())
                if 'keywords_summary' in all_keys:
                    all_keys.remove('keywords_summary')
                    # Insert after confidence
                    if 'confidence' in all_keys:
                        conf_idx = all_keys.index('confidence')
                        all_keys.insert(conf_idx + 1, 'keywords_summary')
                    else:
                        all_keys.append('keywords_summary')
                
                writer = csv.DictWriter(f, fieldnames=all_keys)
                writer.writeheader()
                writer.writerows(leads_data)
    else:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(leads_data, f, indent=2, default=str)
    
    console.print(f"✅ Exported {len(leads_data)} leads to {output_path}", style="green")


if __name__ == "__main__":
    cli()
