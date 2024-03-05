from typing import List, Tuple

import re
import pandas as pd


def parameters(start: pd.Timestamp, end: pd.Timestamp, file_category: str) -> List[Tuple[str, str]]:
    return [("dateStart", start.strftime('%Y-%m-%d')),
            ("dateEnd", end.strftime('%Y-%m-%d')),
            ("FileCategory", file_category)]

def deduplicate_json(json_file, file_category: str, filetype: str = "xlsx"):
    # Step 1: Define a regex pattern to match URLs of the format "*{file_category}_0x.xlsx"
    pattern = re.compile(rf'^(.*){re.escape(file_category)}_0(\d+)\.{re.escape(filetype)}$')

    # Step 2: Iterate through the URLs and extract matching entries and their components (prefix and x)
    entries = {}
    for item in json_file:
        match = pattern.match(item["file_path"])
        if match:
            prefix, x = match.groups()
            x = int(x)  # Convert the extracted number to an integer
            if prefix not in entries or entries[prefix]['x'] < x:
                entries[prefix] = {'x': x, 'file_path': item["file_path"]}

    # Step 3: Keep only the highest 'x' entries for each prefix
    # This has been handled in the loop above by updating the entries dictionary.

    # Step 4: Extract the URLs of the highest 'x' entries
    final_urls = [entry['file_path'] for entry in entries.values()]

    # Step 5: Rebuild the JSON data with the filtered URLs
    filtered_json_data = [{"file_path": url} for url in final_urls]

    return filtered_json_data


