"""
Keyword-based Inventing classifier
Uses sectors + description to assign tech invention categories to each company.
Output: data/companies_classified.csv
"""

import csv
from pathlib import Path

COMPANIES_FILE = Path(__file__).parent.parent / "data" / "companies_filtered.csv"
OUTPUT_FILE    = Path(__file__).parent.parent / "data" / "companies_classified.csv"

# category -> keywords to match (against sectors + description, lowercased)
CATEGORIES = {
    "AI/ML":            ["machine learning", "artificial intelligence", "computer vision",
                         "generative ai", "deep learning", "nlp", "natural language"],
    "Cybersecurity":    ["security", "cybersecurity", "threat", "vulnerability",
                         "encryption", "firewall", "zero trust"],
    "Fintech":          ["fintech", "financial services", "payments", "banking",
                         "insurance", "lending", "trading", "wealth"],
    "Healthtech":       ["healthtech", "biotech", "pharmaceutical", "clinical",
                         "medical", "patient", "health care", "healthcare"],
    "Cloud/Infra":      ["cloud", "infrastructure", "devops", "kubernetes",
                         "microservices", "serverless", "data center"],
    "eCommerce/Retail": ["ecommerce", "e-commerce", "retail", "marketplace",
                         "shopping", "consumer web"],
    "Data/Analytics":   ["analytics", "business intelligence", "big data",
                         "data platform", "data engineering", "visualization"],
    "HR Tech":          ["hr tech", "human resources", "workforce", "recruiting",
                         "talent", "payroll"],
    "Marketing Tech":   ["marketing tech", "adtech", "advertising", "crm",
                         "marketing automation", "growth"],
    "IoT/Hardware":     ["hardware", "iot", "internet of things", "semiconductor",
                         "robotics", "embedded", "sensors", "autonomous"],
    "Defense":          ["defense", "military", "government", "federal",
                         "national security"],
    "Edtech":           ["edtech", "education", "e-learning", "learning platform",
                         "training"],
    "Logistics":        ["logistics", "supply chain", "shipping", "freight",
                         "transportation", "fleet"],
    "Real Estate":      ["real estate", "proptech", "property", "mortgage"],
    "Quantum":          ["quantum", "quantum computing"],
}


def classify(sectors: str, description: str) -> list[str]:
    text = (sectors + " " + description).lower()
    matched = []
    for category, keywords in CATEGORIES.items():
        if any(kw in text for kw in keywords):
            matched.append(category)
    return matched if matched else ["Other"]


def main():
    with open(COMPANIES_FILE, encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    results = []
    for c in companies:
        cats = classify(c.get("sectors", ""), c.get("description", ""))
        results.append({**c, "invent_categories": " | ".join(cats)})

    # Stats
    from collections import Counter
    all_cats = []
    for r in results:
        all_cats.extend(r["invent_categories"].split(" | "))
    counts = Counter(all_cats)

    print("=== Inventing Category Distribution ===")
    for cat, count in counts.most_common():
        print(f"  {cat:<20} {count}")
    print(f"\nTotal: {len(results)} companies")

    # Write output
    fields = list(companies[0].keys()) + ["invent_categories"]
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
