"""
Petroleum Intel - Flask API
REST API for the web dashboard.
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import json
from datetime import datetime
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_cors import CORS

from models.database import get_db
from scrapers import SCRAPER_REGISTRY
from intelligence.scorer import get_scorer
from intelligence.mapper import get_mapper


def create_app():
    """Create and configure Flask application."""
    
    app = Flask(
        __name__,
        template_folder=str(PROJECT_ROOT / "templates"),
        static_folder=str(PROJECT_ROOT / "static"),
    )
    app.config['SECRET_KEY'] = 'petroleum-intel-hackathon-2026'
    
    CORS(app)
    
    # Initialize database
    db = get_db()
    
    # ================== WEB ROUTES ==================
    
    @app.route('/')
    def dashboard():
        """Main dashboard page."""
        return render_template('dashboard.html')
    
    # ================== API ROUTES ==================
    
    @app.route('/api/stats')
    def api_stats():
        """Get dashboard statistics."""
        stats = db.get_stats()
        return jsonify(stats)
    
    @app.route('/api/leads')
    def api_leads():
        """Get leads with filters."""
        # Parse query params
        status = request.args.get('status')
        product = request.args.get('product')
        min_score = request.args.get('min_score', type=float)
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        leads = db.get_leads(
            status=status,
            product=product,
            min_score=min_score,
            limit=limit,
            offset=offset,
        )
        
        # Parse JSON fields
        for lead in leads:
            if lead.get('keywords_matched'):
                try:
                    lead['keywords_matched'] = json.loads(lead['keywords_matched'])
                except:
                    pass
            if lead.get('raw_data'):
                try:
                    lead['raw_data'] = json.loads(lead['raw_data'])
                except:
                    pass
        
        return jsonify({
            'leads': leads,
            'count': len(leads),
            'offset': offset,
        })
    
    @app.route('/api/leads/<int:lead_id>')
    def api_lead_detail(lead_id):
        """Get single lead details."""
        lead = db.get_lead_by_id(lead_id)
        if not lead:
            return jsonify({'error': 'Lead not found'}), 404
        
        # Parse JSON fields
        if lead.get('keywords_matched'):
            try:
                lead['keywords_matched'] = json.loads(lead['keywords_matched'])
            except:
                pass
        if lead.get('raw_data'):
            try:
                lead['raw_data'] = json.loads(lead['raw_data'])
            except:
                pass
        
        return jsonify(lead)
    
    @app.route('/api/leads/<int:lead_id>/status', methods=['POST'])
    def api_update_status(lead_id):
        """Update lead status."""
        data = request.get_json()
        status = data.get('status')
        notes = data.get('notes')
        
        if not status:
            return jsonify({'error': 'Status required'}), 400
        
        db.update_lead_status(lead_id, status, notes)
        return jsonify({'success': True})
    
    @app.route('/api/scrape/<source>', methods=['POST'])
    def api_scrape(source):
        """Trigger a scraper manually."""
        if source not in SCRAPER_REGISTRY and source != 'all':
            return jsonify({'error': 'Unknown source'}), 400
        
        sources = list(SCRAPER_REGISTRY.keys()) if source == 'all' else [source]
        
        results = {}
        scorer = get_scorer()
        mapper = get_mapper()
        
        for src in sources:
            try:
                scraper = SCRAPER_REGISTRY[src]()
                signals = scraper.run()
                
                if signals:
                    signals = mapper.enrich_batch(signals)
                    signals = scorer.score_batch(signals)
                    result = db.insert_leads_batch(signals)
                    results[src] = {
                        'success': True,
                        'found': len(signals),
                        'inserted': result['inserted'],
                        'duplicates': result['duplicates'],
                    }
                else:
                    results[src] = {'success': True, 'found': 0, 'inserted': 0}
                    
            except Exception as e:
                results[src] = {'success': False, 'error': str(e)}
        
        return jsonify(results)
    
    @app.route('/api/sources')
    def api_sources():
        """Get available data sources."""
        from config.settings import SOURCES
        return jsonify(SOURCES)
    
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
