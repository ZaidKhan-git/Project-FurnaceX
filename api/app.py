"""
FurnaceX API - Flask Backend v3
Focused on user-relevant data: Company, Location, Procurement Clues, Products, Urgency, Score
"""

from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import pandas as pd
import numpy as np
import subprocess
import sys
import re
import math
import json
from pathlib import Path
from datetime import datetime, timedelta
import threading

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

def create_app():
    return app

# Base paths
BASE_PATH = Path(__file__).parent.parent
DATA_PATH = BASE_PATH / 'data'
PIPELINE_SCRIPT = BASE_PATH / 'run_full_pipeline.py'

# Pipeline status
pipeline_status = {
    'running': False,
    'last_run': None,
    'result': None,
    'error': None
}


def clean_for_json(obj):
    """Recursively clean NaN/None values for JSON serialization"""
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif pd.isna(obj):
        return None
    return obj


def parse_submission_date(other_details):
    """Extract submission date from Other Details column"""
    if pd.isna(other_details):
        return None
    match = re.search(r'Date of Submission:\s*(\d{2}/\d{2}/\d{4})', str(other_details))
    if match:
        try:
            return datetime.strptime(match.group(1), '%d/%m/%Y')
        except:
            pass
    return None


def extract_procurement_clues(lead):
    """Extract procurement clues from project description"""
    desc = str(lead.get('Project_Description', '')).lower()
    clues = []
    
    # Industry-specific clues
    if 'steel' in desc or 'iron' in desc:
        clues.append("Steel/Iron production requiring furnace fuel")
    if 'power plant' in desc or 'captive power' in desc:
        clues.append("Captive power generation - continuous fuel demand")
    if 'expansion' in desc:
        clues.append("Expansion project - increased fuel requirements")
    if 'greenfield' in desc:
        clues.append("New facility - early supplier opportunity")
    if 'construction' in desc or 'residential' in desc:
        clues.append("Construction activity - diesel & bitumen demand")
    if 'distillery' in desc or 'ethanol' in desc:
        clues.append("Distillery operations - backup fuel needs")
    if 'mining' in desc or 'quarry' in desc:
        clues.append("Mining operations - heavy machinery fuel")
    if 'cement' in desc:
        clues.append("Cement manufacturing - industrial fuel requirement")
    if 'hospital' in desc or 'hotel' in desc:
        clues.append("Commercial facility - generator fuel & maintenance")
    if 'warehouse' in desc or 'logistics' in desc:
        clues.append("Logistics hub - fleet fuel requirements")
    
    if not clues:
        clues.append("General industrial activity")
    
    return clues


def generate_nlp_inference(lead):
    """Generate AI insights from lead data"""
    inferences = []
    desc = str(lead.get('Project_Description', '')).lower()
    score = lead.get('Final_Score') or 0
    freshness = lead.get('Freshness_Score') or 0
    intent = lead.get('Intent_Signal') or 0
    
    if score >= 85:
        inferences.append("⭐ HIGH PRIORITY - Strong signals for immediate engagement")
    if freshness >= 0.9:
        inferences.append("🔥 FRESH LEAD - Recently submitted, first-mover advantage")
    if intent >= 0.7:
        inferences.append("📈 Strong procurement intent detected")
    if 'greenfield' in desc:
        inferences.append("New project offers early partnership opportunity")
    if 'expansion' in desc:
        inferences.append("Growth phase indicates increased demand")
    
    if not inferences:
        inferences.append("Standard lead - follow up per normal process")
    
    return inferences


# Routes
@app.route('/')
def serve_landing():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/app')
def serve_app():
    return send_from_directory(app.static_folder, 'app.html')


@app.route('/lead')
def serve_lead_detail():
    return send_from_directory(app.static_folder, 'lead.html')


@app.route('/api/status')
def get_status():
    return jsonify(pipeline_status)


@app.route('/api/pipeline/run', methods=['POST'])
def run_pipeline():
    global pipeline_status
    
    if pipeline_status['running']:
        return jsonify({'error': 'Pipeline already running'}), 409
    
    def execute_pipeline():
        global pipeline_status
        pipeline_status['running'] = True
        pipeline_status['error'] = None
        
        try:
            result = subprocess.run(
                [sys.executable, str(PIPELINE_SCRIPT)],
                cwd=str(BASE_PATH),
                capture_output=True,
                text=True,
                timeout=300
            )
            pipeline_status['result'] = {
                'success': result.returncode == 0,
                'stdout': result.stdout[-2000:] if result.stdout else ''
            }
            pipeline_status['last_run'] = datetime.now().isoformat()
        except Exception as e:
            pipeline_status['error'] = str(e)
        finally:
            pipeline_status['running'] = False
    
    thread = threading.Thread(target=execute_pipeline)
    thread.start()
    
    return jsonify({'message': 'Pipeline started'})


@app.route('/api/leads')
def get_leads():
    """Get leads with filtering - returns user-relevant columns"""
    filter_type = request.args.get('filter', 'all')
    product = request.args.get('product', '')
    age_filter = request.args.get('age', '')
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    
    csv_path = DATA_PATH / 'filtered_dataset.csv'
    
    if not csv_path.exists():
        return jsonify({'error': 'No data available', 'leads': [], 'total': 0}), 200
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        total_all = len(df)
        
        # Parse dates for age filter
        df['_parsed_date'] = df['Other Details'].apply(parse_submission_date)
        now = datetime.now()
        
        # Age filter
        if age_filter == '3m':
            cutoff = now - timedelta(days=90)
            df = df[df['_parsed_date'] >= cutoff]
        elif age_filter == '6m':
            cutoff = now - timedelta(days=180)
            df = df[df['_parsed_date'] >= cutoff]
        elif age_filter == '1y':
            cutoff = now - timedelta(days=365)
            df = df[df['_parsed_date'] >= cutoff]
        
        # Tier filter
        if filter_type == 'tier1':
            df = df[df['Priority_Tier'] == 'Tier 1 - Immediate']
        elif filter_type == 'tier2':
            df = df[df['Priority_Tier'] == 'Tier 2 - High Priority']
        elif filter_type == 'top5':
            df = df.nlargest(5, 'Final_Score')
        
        # Product filter
        if product:
            df = df[df['Recommended_Products'].str.contains(product, case=False, na=False)]
        
        # Sort by score
        df = df.sort_values('Final_Score', ascending=False)
        
        total_filtered = len(df)
        
        # Pagination
        df = df.iloc[offset:offset + limit]
        
        # Build response with user-relevant columns
        leads = []
        for _, row in df.iterrows():
            lead = {
                'ID': row.get('ID', ''),
                'Company_Name': row.get('Company_Name', 'Unknown'),
                'Location': f"{row.get('Location', '')} - {row.get('State', '')}".strip(' -'),
                'Procurement_Clues': extract_procurement_clues(row.to_dict()),
                'Recommended_Products': row.get('Recommended_Products', ''),
                'Urgency': 'HIGH' if row.get('High_Urgency_Flag') else 'NORMAL',
                'Priority_Tier': row.get('Priority_Tier', 'Tier 2 - High Priority'),
                'Confidence_Score': round(row.get('Final_Score', 0) or 0, 1),
                'Signal_Type': row.get('Signal_Type', ''),
                'State': row.get('State', '')
            }
            leads.append(clean_for_json(lead))
        
        return jsonify({
            'leads': leads,
            'total': total_filtered,
            'total_all': total_all,
            'offset': offset,
            'limit': limit,
            'has_more': offset + limit < total_filtered
        })
        
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/api/leads/<path:lead_id>')
def get_lead_detail(lead_id):
    """Get full lead details"""
    csv_path = DATA_PATH / 'filtered_dataset.csv'
    
    if not csv_path.exists():
        return jsonify({'error': 'No data available'}), 404
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        lead_row = df[df['ID'] == lead_id]
        
        if lead_row.empty:
            return jsonify({'error': 'Lead not found'}), 404
        
        row = lead_row.iloc[0]
        
        # Build comprehensive lead detail
        lead = {
            'ID': row.get('ID', ''),
            'Company_Name': row.get('Company_Name', 'Unknown'),
            'Location': row.get('Location', ''),
            'State': row.get('State', ''),
            'Signal_Type': row.get('Signal_Type', ''),
            'Source_URL': row.get('Source_URL', ''),
            'Project_Name': row.get('Project Name', ''),
            'Project_Description': row.get('Project_Description', ''),
            'Sector': row.get('Sector', ''),
            
            # Scores
            'Confidence_Score': round(row.get('Final_Score', 0) or 0, 1),
            'Freshness_Score': round((row.get('Freshness_Score', 0) or 0) * 100, 0),
            'Intent_Signal': round((row.get('Intent_Signal', 0) or 0) * 100, 0),
            'Proximity_Score': round((row.get('Proximity_Score', 0) or 0) * 100, 0),
            
            # Priority
            'Priority_Tier': row.get('Priority_Tier', ''),
            'Urgency': 'HIGH' if row.get('High_Urgency_Flag') else 'NORMAL',
            
            # Products & Clues
            'Recommended_Products': row.get('Recommended_Products', ''),
            'Procurement_Clues': extract_procurement_clues(row.to_dict()),
            'NLP_Insights': generate_nlp_inference(row.to_dict()),
            
            # Officer info
            'Officer_Name': row.get('Officer_Name', 'Not assigned'),
            'Officer_Phone': row.get('Officer_Phone', ''),
            'Officer_Email': row.get('Officer_Email', ''),
            'Officer_Address': row.get('Officer_Address', ''),
            'Officer_Role': row.get('Officer_Role', ''),
            
            # Dates
            'Submission_Year': row.get('Submission_Year', '')
        }
        
        # Parse submission date
        parsed_date = parse_submission_date(row.get('Other Details', ''))
        lead['Submission_Date'] = parsed_date.strftime('%d %b %Y') if parsed_date else 'Unknown'
        
        return jsonify(clean_for_json(lead))
        
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/api/products')
def get_products():
    csv_path = DATA_PATH / 'filtered_dataset.csv'
    
    if not csv_path.exists():
        return jsonify({'products': []})
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        all_products = set()
        for products_str in df['Recommended_Products'].dropna():
            products = [p.strip() for p in str(products_str).split(',')]
            all_products.update(products)
        all_products.discard('')
        return jsonify({'products': sorted(list(all_products))})
    except:
        return jsonify({'products': []})


@app.route('/api/stats')
def get_stats():
    csv_path = DATA_PATH / 'filtered_dataset.csv'
    
    if not csv_path.exists():
        return jsonify({'total_leads': 0})
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        df['_parsed_date'] = df['Other Details'].apply(parse_submission_date)
        now = datetime.now()
        
        return jsonify({
            'total_leads': len(df),
            'tier1_count': len(df[df['Priority_Tier'] == 'Tier 1 - Immediate']),
            'tier2_count': len(df[df['Priority_Tier'] == 'Tier 2 - High Priority']),
            'leads_3m': len(df[df['_parsed_date'] >= now - timedelta(days=90)]),
            'leads_6m': len(df[df['_parsed_date'] >= now - timedelta(days=180)]),
            'high_urgency': int(df['High_Urgency_Flag'].sum()) if 'High_Urgency_Flag' in df.columns else 0
        })
    except:
        return jsonify({'total_leads': 0})


if __name__ == '__main__':
    print("=" * 60)
    print("🔥 FurnaceX API Server v3")
    print("=" * 60)
    print(f"http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
