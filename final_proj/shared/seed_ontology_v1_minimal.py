"""
Seed a minimal ontology v1 graph for runtime/planning verification.

This intentionally creates one compact graph that covers:
- all required node labels
- all required relationship types
- required FK/SAFE_JOIN/RISKY_JOIN properties
"""

from __future__ import annotations

from neo4j_client import run_query, close


def main() -> None:
    run_query("MATCH (n) DETACH DELETE n")

    run_query(
        """
        MERGE (db:Database {db_id: 'demo_warehouse'})
        SET db.dialect = 'snowflake', db.benchmark = 'Spider2'

        MERGE (orders:Table {db_id: 'demo_warehouse', name: 'orders'})
        SET orders.row_count = 100000, orders.pagerank = 0.85, orders.community_id = 1
        MERGE (customers:Table {db_id: 'demo_warehouse', name: 'customers'})
        SET customers.row_count = 50000, customers.pagerank = 0.92, customers.community_id = 1
        MERGE (events:Table {db_id: 'demo_warehouse', name: 'events'})
        SET events.row_count = 250000, events.pagerank = 0.60, events.community_id = 2

        MERGE (db)-[:HAS]->(orders)
        MERGE (db)-[:HAS]->(customers)
        MERGE (db)-[:HAS]->(events)

        MERGE (c_order_customer:Column {db_id: 'demo_warehouse', table_name: 'orders', name: 'customer_id'})
        SET c_order_customer.type = 'TEXT'
        MERGE (c_customer_id:Column {db_id: 'demo_warehouse', table_name: 'customers', name: 'customer_id'})
        SET c_customer_id.type = 'TEXT'
        MERGE (c_order_amount:Column {db_id: 'demo_warehouse', table_name: 'orders', name: 'order_amount'})
        SET c_order_amount.type = 'NUMBER'

        MERGE (orders)-[:HAS]->(c_order_customer)
        MERGE (orders)-[:HAS]->(c_order_amount)
        MERGE (customers)-[:HAS]->(c_customer_id)

        MERGE (c_order_customer)-[fk:FK]->(c_customer_id)
        SET fk.enforced = true, fk.confidence = 1.0

        MERGE (orders)-[sj:SAFE_JOIN]->(customers)
        SET sj.join_keys = ['orders.customer_id=customers.customer_id'],
            sj.cardinality = 'N:1',
            sj.risk_score = 0.10,
            sj.confidence = 0.98,
            sj.evidence = 'declared_fk'

        MERGE (events)-[rj:RISKY_JOIN]->(orders)
        SET rj.reason = 'low_confidence_event_link',
            rj.severity = 'high'

        MERGE (md:MetricDefinition {db_id: 'demo_warehouse', name: 'Net Revenue Retention'})
        SET md.canonical_sql_template = 'SUM(order_amount) / SUM(previous_period_amount)',
            md.definition = 'Revenue retention for active customers',
            md.default_time_grain = 'month'

        MERGE (bt:BusinessTerm {db_id: 'demo_warehouse', name: 'active_customer'})
        MERGE (dd:DataDomain {name: 'finance'})
        MERGE (bt)-[:IN_DOMAIN]->(dd)
        MERGE (orders)-[:IN_DOMAIN]->(dd)
        MERGE (customers)-[:IN_DOMAIN]->(dd)

        MERGE (md)-[:USES_TABLE]->(orders)
        MERGE (md)-[:USES_COLUMN]->(c_order_amount)
        MERGE (md)-[:REQUIRES_TERM]->(bt)

        MERGE (qp:QueryPattern {db_id: 'demo_warehouse', pattern_id: 'seed_pattern'})
        SET qp.intent_type = 'trend',
            qp.question_template = 'monthly nrr by industry',
            qp.success_rate = 1.0
        MERGE (qp)-[:USES_TABLE]->(orders)
        MERGE (qp)-[:USES_METRIC]->(md)

        MERGE (fm:FailureMode {db_id: 'demo_warehouse', name: 'fanout_join'})
        SET fm.description = 'duplicate rows from many-to-many join',
            fm.detection_rule = 'join_cardinality_many_to_many',
            fm.mitigation_rule = 'route_over_safe_join'
        MERGE (qp)-[:TRIGGERED_FAILURE]->(fm)
        MERGE (fm)-[:AFFECTS]->(orders)
        MERGE (fm)-[:AFFECTS]->(md)
        """
    )

    print("SEEDED demo_warehouse ontology graph.")
    close()


if __name__ == "__main__":
    main()
