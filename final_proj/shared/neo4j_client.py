import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        uri = os.environ["NEO4J_URI"]
        user = os.environ["NEO4J_USERNAME"]
        password = os.environ["NEO4J_PASSWORD"]
        _driver = GraphDatabase.driver(uri, auth=(user, password))
    return _driver


def run_query(cypher, params=None):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, params or {})
        return result.data()


def close():
    global _driver
    if _driver:
        _driver.close()
        _driver = None


if __name__ == "__main__":
    result = run_query("RETURN 1 AS ok")
    assert result[0]["ok"] == 1
    print("Neo4j connection OK")
    close()
