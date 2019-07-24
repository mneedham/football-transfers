from neo4j import GraphDatabase
from pathvalidate import sanitize_filename

driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))

with driver.session() as session:
    query = """
    match (l:League)
    return l.href AS href, l.name AS name
    """
    result = session.run(query)
    for row in result:
        new_id = sanitize_filename(row['href'])
        session.run("""
        MATCH (l:League {href: {href} })
        SET l.id = {id}""", {"href": row["href"], "id": new_id})