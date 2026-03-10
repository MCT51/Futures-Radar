import os
import sys
import csv
import glob
import requests

HF_API_URL = "https://router.huggingface.co/hf-inference/models/cardiffnlp/twitter-roberta-base-sentiment-latest"
# Labels returned by this model
LABELS = ["negative", "neutral", "positive"]
# Maximum characters sent to the API per article (well within the token limit)
MAX_CHARS = 1024


def _hf_headers():
    token = os.environ.get("HF_API_TOKEN", "")
    return {"Authorization": f"Bearer {token}"} if token else {}


def analyse_text(text):
    """Call the HuggingFace Inference API and return sentiment scores.

    Returns a dict with keys: negative, neutral, positive, top_label, top_score.
    Returns None on failure.
    """
    payload = {"inputs": text[:MAX_CHARS]}
    for attempt in range(3):
        try:
            resp = requests.post(HF_API_URL, headers=_hf_headers(), json=payload, timeout=30)
            resp.raise_for_status()
            raw = resp.json()
            # Response shape: [[{"label": "negative", "score": 0.7}, ...]]
            items = raw[0] if isinstance(raw[0], list) else raw
            scores = {item["label"].lower(): item["score"] for item in items}
            neg = scores.get("negative", 0.0)
            neu = scores.get("neutral", 0.0)
            pos = scores.get("positive", 0.0)
            top_label = max(scores, key=scores.get)
            return {
                "negative": neg,
                "neutral": neu,
                "positive": pos,
                "top_label": top_label,
                "top_score": scores[top_label],
            }
        except Exception as exc:
            print(f"  API error (attempt {attempt + 1}): {exc}")
            time.sleep(5)
    return None


def analyse_file(filepath):
    """Read a .txt file and return its sentiment analysis."""
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read().strip()
    if not text:
        return None
    result = analyse_text(text)
    if result:
        result["file"] = os.path.basename(filepath)
    return result


def main():
    if len(sys.argv) > 1:
        folder = sys.argv[1]
    else:
        folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "articles")

    if not os.path.isdir(folder):
        print(f"Error: folder '{folder}' does not exist.")
        print("Usage: python sentiment.py [folder_path]")
        sys.exit(1)

    txt_files = sorted(glob.glob(os.path.join(folder, "*.txt")))
    if not txt_files:
        print(f"No .txt files found in '{folder}'.")
        sys.exit(0)

    print(f"Analysing {len(txt_files)} file(s) via HuggingFace Inference API...\n")

    results = []
    for filepath in txt_files:
        result = analyse_file(filepath)
        if result is None:
            print(f"  {os.path.basename(filepath)}: (empty or API error, skipped)")
            continue
        results.append(result)
        print(
            f"  {result['file']}: {result['top_label']} "
            f"(neg={result['negative']:.3f}, neu={result['neutral']:.3f}, "
            f"pos={result['positive']:.3f})"
        )

    if results:
        output_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sentiment_results.csv")
        fieldnames = ["file", "chunk", "top_label", "top_score", "negative", "neutral", "positive"]
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow({
                    "file": result["file"],
                    "chunk": "avg",
                    "top_label": result["top_label"],
                    "top_score": result["top_score"],
                    "negative": result["negative"],
                    "neutral": result["neutral"],
                    "positive": result["positive"],
                })
        print(f"\nResults saved to {output_csv}")


if __name__ == "__main__":
    main()
