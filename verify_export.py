
import csv
import json

def verify_csv():
    print("Verifying leads_export.csv for Credit Rating Signals...")
    try:
        with open('data/leads_export.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)
            
        rows = [r for r in all_rows if r['signal_type'] == 'Credit Rating Rationale']
        ncr_rows = [r for r in all_rows if r['signal_type'] == 'New Company Registration']
            
        print(f"Found {len(rows)} Credit Rating Rationale leads.")
        
        for i, r in enumerate(rows[:3], 1):
            print(f"\nLead {i}: {r['company_name']}")
            print(f"  Signal: {r['signal_type']}")
            raw = json.loads(r['raw_data'])
            print(f"  Loan Purpose: {raw.get('loan_purpose')}")
            
            fuel_signals = raw.get('fuel_signals', [])
            if fuel_signals:
                fs = fuel_signals[0]
                print(f"  ✨ Signal: {fs.get('signal')} -> Product: {fs.get('product')}")
            else:
                print("  ❌ No fuel signals found")
                
            print(f"  Mapped Product: {r['product_match']}")
            
        # Verify New Company Registrations
        print(f"\nFound {len(ncr_rows)} New Company Registration leads.")
        
        for i, r in enumerate(ncr_rows[:3], 1):
            print(f"\nLead {i}: {r['company_name']}")
            print(f"  Signal: {r['signal_type']}")
            raw = json.loads(r['raw_data'])
            print(f"  Capital: {raw.get('authorized_capital_cr', 'N/A')} Cr")
            print(f"  Inferred Needs: {raw.get('inferred_needs')}")
            print(f"  Mapped Product: {r['product_match']}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_csv()
