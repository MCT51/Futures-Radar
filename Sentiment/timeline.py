#use sentiment analysis to create a timeline of sentiment scores for a series of text files, where each file represents a different time period (e.g. from different months). The timeline should show how sentiment changes over time.
import os
import glob
import re
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sentiment import analyse_file, load_model

def create_timeline(folder):
    if not os.path.isdir(folder):
        print(f"Error: folder '{folder}' does not exist.")
        return

    # TODO: need to figure out how to encode the date - assume that the filename contains a date in YYYY-MM format, e.g. "2023-01.txt"
    txt_files = sorted(glob.glob(os.path.join(folder, "*.txt")))
    if not txt_files:
        print(f"No .txt files found in '{folder}'.")
        return

    print(f"Loading model...")
    tokenizer, model = load_model()
    print(f"Analysing {len(txt_files)} file(s)...\n")

    timeline = []
    for filepath in txt_files:
        filename = os.path.basename(filepath)
        match = re.search(r'(\d{4}-\d{2})', filename)
        if not match:
            print(f"  {filename}: skipping (filename does not contain YYYY-MM date)")
            continue
        date = datetime.strptime(match.group(1), "%Y-%m")

        result = analyse_file(filepath, tokenizer, model)
        if result is None:
            print(f"  {filename}: (empty file, skipped)")
            continue
        timeline.append((date, result["top_score"]))

    # Plotting the timeline
    if timeline:
        dates, scores = zip(*timeline)
        plt.figure(figsize=(10, 5))
        plt.plot(dates, scores, marker='o')
        plt.title('Sentiment Timeline')
        plt.xlabel('Date')
        plt.ylabel('Sentiment Score')
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.xticks(rotation=45)
        plt.grid()
        plt.tight_layout()
        plt.show()
    else:
        print("No valid sentiment scores to plot.") 


create_timeline("education_news")