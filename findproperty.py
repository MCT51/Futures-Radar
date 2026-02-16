import json
from pathlib import Path
g = json.load(open(Path("Heatmap") / "heatmap boundaries.geojson","r",encoding="utf-8"))
props = g["features"][0]["properties"].keys()
print(list(props))
for k in props:
    vals = [f["properties"].get(k) for f in g["features"][:50]]
    print(k, "— non-null sample:", next((v for v in vals if v not in (None,"")), None))
