python3 << 'EOF'
import json
TICKER = "ABNB"
with open(f"data/raw_json/{TICKER}_raw.json") as f:
    data = json.load(f)
sec_data = data.get('sec_data', data)
facts = sec_data.get('facts', {}).get('us-gaap', {})
print(f"\n{TICKER} XBRL Tags:")
for concept in sorted(facts.keys()):
    units = facts[concept].get('units', {})
    for unit_type, data_points in units.items():
        annual = [d for d in data_points if d.get('form') == '10-K']
        if annual:
            print(concept)
            break
EOF
