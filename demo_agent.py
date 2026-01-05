import json
import glob
import os

class IsaacAgent:
    def __init__(self, record_path="examples/*.json"):
        self.kb = []
        self._load_knowledge_base(record_path)

    def _load_knowledge_base(self, pattern):
        print(f"🤖 AGENT: Loading Knowledge Base from {pattern}...")
        files = glob.glob(pattern)
        for f in files:
            with open(f, 'r') as fp:
                rec = json.load(fp)
                self.kb.append(rec)
        print(f"🤖 AGENT: Ingested {len(self.kb)} records.\n")

    def query(self, domain=None, min_fe=None, material_contains=None):
        results = []
        for rec in self.kb:
            # 1. Filter by Domain
            if domain and rec.get('record_domain') != domain:
                continue

            # 2. Filter by Material
            if material_contains:
                mat_name = rec.get('sample', {}).get('material', {}).get('name', '')
                if material_contains.lower() not in mat_name.lower():
                    continue

            # 3. Filter by Performance Metric (FE)
            if min_fe:
                # Agent looks into descriptors for "faradaic_efficiency"
                descriptors = rec.get('descriptors', {}).get('outputs', [])[0].get('descriptors', [])
                fe_vals = [d for d in descriptors if 'faradaic_efficiency' in d['name']]
                
                # Check if any FE descriptor meets criteria
                pass_fe = False
                for fe in fe_vals:
                    if fe['value'] >= min_fe:
                        pass_fe = True
                        break
                if not pass_fe:
                    continue

            results.append(rec)
        return results

    def explain(self, records):
        if not records:
            print("🤖 AGENT: No records matching criteria found.")
            return

        print(f"🤖 AGENT: Found {len(records)} matching records:")
        for r in records:
            rid = r['record_id']
            rtype = r['record_domain']
            mat = r['sample']['material']['name']
            print(f"   📄 [{rtype.upper()}] {rid} | Material: {mat}")
            
            # Summarize interesting descriptors
            descs = r['descriptors']['outputs'][0]['descriptors']
            print("      💡 Key Insights:")
            for d in descs[:3]: # Show top 3
                val = d['value']
                unit = d.get('unit', '')
                print(f"         - {d['name']}: {val} {unit}")
            print("")

if __name__ == "__main__":
    agent = IsaacAgent()

    print("--- 🔬 Query 1: Find all Characterization records involving Copper ---")
    cu_recs = agent.query(domain="characterization", material_contains="Cu")
    agent.explain(cu_recs)

    print("--- ⚡️ Query 2: Find Catalysts with Faradaic Efficiency > 40% ---")
    high_perf = agent.query(min_fe=0.40)
    agent.explain(high_perf)

    print("--- 🧠 Query 3: Find Simulation records ---")
    sims = agent.query(domain="simulation")
    agent.explain(sims)
