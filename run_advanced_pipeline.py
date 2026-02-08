"""
Advanced Intelligence Pipeline Demo.
Showcases Product-Need Inference, Confidence Scoring, and Geospatial Routing.
"""
import sys
from datetime import datetime
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.append(str(__file__).rsplit('\\', 2)[0])

from intelligence.inference import ProductInferenceEngine
from intelligence.scorer import LeadScorer
from intelligence.routing import GeospatialRouter

console = Console()

def run_pipeline():
    inference_engine = ProductInferenceEngine()
    scorer = LeadScorer()
    router = GeospatialRouter()
    
    # Test Cases from User Prompt + More
    test_signals = [
        {
            "text": "Looking for fuel suppliers for our new road project in Bihar.",
            "source": "Government Tender",
        },
        {
            "text": "We operate a glass factory in Gujarat and need heating solutions.",
            "source": "Inquiry",
        },
        {
            "text": "Urgent requirement for 500 KVA Diesel Generator set for backup power in Delhi office.",
            "source": "Procurement Notice",
        },
        {
            "text": "Textile mill in Surat expanding capacity.",
            "source": "News Article",
        },
        {
            "text": "Supply of VG-30 Bitumen for highway resurfacing in Punjab.",
            "source": "Government Tender",
        }
    ]
    
    console.print("[bold blue]🚀 Advanced Intelligence Pipeline Running...[/bold blue]\n")
    
    table = Table(title="Lead Intelligence Report")
    table.add_column("Signal Text", style="cyan", no_wrap=False)
    table.add_column("Inferred Product", style="green")
    table.add_column("Conf.", justify="right")
    table.add_column("Score", justify="right")
    table.add_column("Routing", style="magenta")
    
    for signal in test_signals:
        text = signal["text"]
        
        # 1. Product Inference
        inference = inference_engine.analyze_signal(text)
        products = inference["products"]
        primary_product = products[0] if products else {"product": "Unknown", "confidence": 0}
        
        # 2. Location & Routing
        route_info = router.route_lead(text)
        
        # 3. Scoring (Weighted)
        lead_data = {
            "signal_type": signal["source"],
            "description": text,
            "raw_data": text,
            "discovered_at": datetime.now(),
        }
        score = scorer.score_lead(lead_data)
        
        # Display
        product_str = f"{primary_product['product']}\n({primary_product.get('reason', '')})"
        route_str = f"{route_info['territory']}\n({route_info.get('sales_officer_id', 'Unassigned')})"
        
        table.add_row(
            text,
            product_str,
            f"{primary_product['confidence']}%",
            f"{score}",
            route_str
        )
        
    console.print(table)
    
    # Explain logic
    console.print("\n[bold yellow]Logic Explanation:[/bold yellow]")
    console.print("1. [bold]Product Inference[/bold]: Uses regex rules (e.g., 'Road Project' -> Bitumen).")
    console.print("2. [bold]Confidence[/bold]: Direct mentions = 95%, Inferred = 60%.")
    console.print("3. [bold]Scoring[/bold]: Weighted (Intent 50% + Size 20% + Recency 30%).")
    console.print("4. [bold]Routing[/bold]: Maps extracted state/city to 'Territory Polygons' (North/South/East/West).")

if __name__ == "__main__":
    run_pipeline()
