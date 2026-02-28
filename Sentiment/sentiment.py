import os
import sys
import csv
import glob
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import numpy as np
from scipy.special import softmax

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
LABELS = ["negative", "neutral", "positive"]
MAX_TOKENS = 512
OVERLAP_TOKENS = 128  # overlap between chunks to preserve context at boundaries


def load_model():
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    return tokenizer, model


def chunk_text(text, tokenizer, max_tokens=MAX_TOKENS, overlap=OVERLAP_TOKENS):
    """Split text into overlapping token chunks that fit the model's limit."""
    token_ids = tokenizer.encode(text, add_special_tokens=False)

    if len(token_ids) <= max_tokens - 2:  # -2 for special tokens (<s> and </s>)
        return [text]

    chunks = []
    stride = max_tokens - 2 - overlap  # step forward by this many tokens each chunk
    for start in range(0, len(token_ids), stride):
        chunk_ids = token_ids[start : start + max_tokens - 2]
        chunk_text = tokenizer.decode(chunk_ids, skip_special_tokens=True)
        chunks.append(chunk_text)
        if start + max_tokens - 2 >= len(token_ids):
            break

    return chunks


def analyse_chunk(text, tokenizer, model):
    """Analyse sentiment of a single chunk. Returns raw softmax scores."""
    encoded = tokenizer(text, return_tensors="pt", truncation=True, max_length=MAX_TOKENS)
    output = model(**encoded)
    scores = output.logits[0].detach().numpy()
    return softmax(scores)


def analyse_text(text, tokenizer, model):
    """Analyse sentiment of a text string, chunking if needed. Returns dict with label scores and top label."""
    chunks = chunk_text(text, tokenizer)
    num_chunks = len(chunks)

    all_scores = np.array([analyse_chunk(chunk, tokenizer, model) for chunk in chunks])
    avg_scores = all_scores.mean(axis=0)

    # Per-chunk breakdown
    chunk_details = []
    for i, scores in enumerate(all_scores):
        chunk_details.append({
            "chunk": i + 1,
            "top_label": LABELS[int(np.argmax(scores))],
            "negative": float(scores[0]),
            "neutral": float(scores[1]),
            "positive": float(scores[2]),
        })

    result = {label: float(avg_scores[i]) for i, label in enumerate(LABELS)}
    result["top_label"] = LABELS[int(np.argmax(avg_scores))]
    result["top_score"] = float(np.max(avg_scores))
    result["chunks"] = num_chunks
    result["chunk_details"] = chunk_details
    return result


def analyse_file(filepath, tokenizer, model):
    """Read a .txt file and return its sentiment analysis."""
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read().strip()

    if not text:
        return None

    result = analyse_text(text, tokenizer, model)
    result["file"] = os.path.basename(filepath)
    return result


def main():
    # Determine input folder: use command-line arg or default to ./txt_files
    if len(sys.argv) > 1:
        folder = sys.argv[1]
    else:
        folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "articles")

    if not os.path.isdir(folder):
        print(f"Error: folder '{folder}' does not exist.")
        print(f"Usage: python sentiment.py [folder_path]")
        print(f"Default folder: ./articles")
        sys.exit(1)

    txt_files = sorted(glob.glob(os.path.join(folder, "*.txt")))
    if not txt_files:
        print(f"No .txt files found in '{folder}'.")
        sys.exit(1)

    print(f"Loading model: {MODEL_NAME}")
    tokenizer, model = load_model()
    print(f"Analysing {len(txt_files)} file(s)...\n")

    results = []
    for filepath in txt_files:
        result = analyse_file(filepath, tokenizer, model)
        if result is None:
            print(f"  {os.path.basename(filepath)}: (empty file, skipped)")
            continue

        results.append(result)
        chunk_info = f" [{result['chunks']} chunk(s)]" if result["chunks"] > 1 else ""
        print(f"  {result['file']}: {result['top_label']} "
              f"(neg={result['negative']:.3f}, neu={result['neutral']:.3f}, pos={result['positive']:.3f}){chunk_info}")

        # Show per-chunk breakdown for multi-chunk files
        if result["chunks"] > 1:
            for cd in result["chunk_details"]:
                print(f"    chunk {cd['chunk']}: {cd['top_label']} "
                      f"(neg={cd['negative']:.3f}, neu={cd['neutral']:.3f}, pos={cd['positive']:.3f})")

    # Write results to CSV (one row per chunk, plus an "average" row per file)
    if results:
        output_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sentiment_results.csv")
        fieldnames = ["file", "chunk", "top_label", "top_score", "negative", "neutral", "positive"]
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                # Write per-chunk rows
                for cd in result["chunk_details"]:
                    writer.writerow({
                        "file": result["file"],
                        "chunk": cd["chunk"],
                        "top_label": cd["top_label"],
                        "top_score": max(cd["negative"], cd["neutral"], cd["positive"]),
                        "negative": cd["negative"],
                        "neutral": cd["neutral"],
                        "positive": cd["positive"],
                    })
                # Write average row for multi-chunk files
                if result["chunks"] > 1:
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
