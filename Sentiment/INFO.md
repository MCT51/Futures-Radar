# Sentiment Analysis Module

## Overview

This module performs sentiment analysis on collections of `.txt` article files and produces both a CSV of scores and a timeline visualisation of how sentiment changes over time.

## Files

| File | Purpose |
|------|---------|
| `sentiment.py` | Core sentiment analysis — scores individual `.txt` files and writes results to `sentiment_results.csv` |
| `timeline.py` | Reads a folder of date-named files and plots a sentiment timeline chart |

## How to Run

**Analyse a folder of articles:**
```bash
python sentiment.py [folder_path]
```
Defaults to the `articles/` folder if no argument is given.

**Plot a sentiment timeline:**
```bash
python timeline.py
```
Currently hardcoded to run on the `education_news/` folder. Edit the last line of `timeline.py` to change the target folder.

## Model

Uses [`cardiffnlp/twitter-roberta-base-sentiment-latest`](https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest) via HuggingFace Transformers.

- Labels: `negative`, `neutral`, `positive`
- Max token limit: 512 tokens per chunk
- Long texts are split into overlapping chunks (128-token overlap) and averaged

## Input Format

- Plain `.txt` files, one article per file
- For the timeline, filenames must contain a date in `YYYY-MM` format (e.g. `2023-01.txt`)

## Output

- `sentiment_results.csv` — one row per chunk, plus an `avg` row for multi-chunk files
  - Columns: `file`, `chunk`, `top_label`, `top_score`, `negative`, `neutral`, `positive`
- A matplotlib line chart (from `timeline.py`) showing sentiment score over time

## Data Folders

| Folder | Contents |
|--------|---------|
| `articles/` | Recent scraped articles (default input for `sentiment.py`) |
| `education_news/` | Monthly education news files (used by `timeline.py`) |
| `txt_files/` | Test files for development |
| `helena_first_files/` | Earlier batch of article files |
