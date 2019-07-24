from neo4j import GraphDatabase
from pathvalidate import sanitize_filename, sanitize_filepath
import os
import requests

driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))

with driver.session() as session:
    query = """
    match (l:League)
    return l.id AS id, l.name AS name
    """
    result = session.run(query)
    for row in result:
        url = f"https://www.transfermarkt.co.uk{row['id']}"
        path = f"data/leagues/{sanitize_filename(row['id'])}.html"
        if not os.path.isfile(path):
            headers = {'user-agent': 'my-app/0.0.1'}
            response = requests.get(url, stream=True, headers=headers)
            with open(path, "wb") as handle:
                for data in response.iter_content():
                    handle.write(data)