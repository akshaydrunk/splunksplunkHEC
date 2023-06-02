import os
import pandas as pd
import requests
import json
from tqdm import tqdm
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG_DIR = "configs"
CONFIG_FILE = "splnkHEC"


if not os.path.exists(CONFIG_DIR):
    os.makedirs(CONFIG_DIR)

config_path = os.path.join(CONFIG_DIR, CONFIG_FILE)


if os.path.exists(config_path):
    with open(config_path) as file:
        config_data = json.load(file)
        url = config_data.get("url")
        token = config_data.get("token")
else:
    cluster_url = input("Cluster URL: ")
    url = f"https://{cluster_url}:8088/services/collector/event"
    token = input("Token: ")


    with open(config_path, "w") as file:
        config_data = {
            "url": url,
            "token": token
        }
        json.dump(config_data, file)


json_file = input("JSON file path: ")
send_null = input("Send null values? (y/n): ").lower() == "y"

with open(json_file) as file:
    file_contents = file.read()
    data = pd.read_json(file_contents, lines=True)

parsed_data = []

for row in data.itertuples():
    line_data = {}
    for key, value in row._asdict().items():
        if pd.isna(value):
            if send_null:
                line_data[key] = None
        else:
            line_data[key] = value
    parsed_data.append(line_data)

headers = {
    "Authorization": f"Splunk {token}"
}

pbar = tqdm(total=len(parsed_data), ncols=100, desc="Sending data to Splunk", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}')
for logs in parsed_data:
    payload = {
        "event": logs
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)

    if response.status_code == 200:
        pbar.set_postfix(success="Sent")
    else:
        pbar.set_postfix(failure="Failed")

    pbar.update(1)

pbar.close()
print("Data sent successfully to Splunk.")
