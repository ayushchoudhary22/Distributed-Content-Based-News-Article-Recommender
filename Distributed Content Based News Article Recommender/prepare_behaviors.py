import csv

# Open the raw behaviors file and an output file for prepared data
with open("behaviors.tsv", "r") as fin, open("behaviors_prepared.txt", "w") as fout:
    reader = csv.reader(fin, delimiter="\t")
    for row in reader:
        if not row:
            continue  # skip empty lines if any

        user_id = row[1]  # second column is user ID
        impression_items = row[4]  # fifth column contains the impressions string

        # The impressions field may contain multiple "newsID-clickLabel" entries separated by spaces.
        # Split by space to get each "<news_id>-<label>" and remove the '-0' or '-1' suffix.
        doc_ids = [impression.split('-')[0] for impression in impression_items.split()]

        # Write out user_id and list of doc_ids (as a Python list literal for ease of parsing later)
        fout.write(f"{user_id}\t{doc_ids}\n")
