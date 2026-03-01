from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pymongo import MongoClient
from psycopg2.extras import execute_batch

MONGO_URI = "mongodb://airflow:airflow@mongo:27017/?authSource=admin"


def replaction_cb():
    mongo = MongoClient(MONGO_URI)
    db = mongo["airflow"]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        TRUNCATE
        marketplace.user_session_pages,
        marketplace.user_session_actions,
        marketplace.user_sessions,
        marketplace.event_log_details,
        marketplace.event_logs,
        marketplace.support_ticket_messages,
        marketplace.support_tickets,
        marketplace.user_recommendation_products,
        marketplace.user_recommendations,
        marketplace.moderation_flags,
        marketplace.moderation_queue
        CASCADE;
    """
    )

    sessions_data = []
    pages_data = []
    actions_data = []

    for s in db["UserSessions"].find():
        sessions_data.append(
            (s["session_id"], s["user_id"], s["start_time"], s["end_time"], s["device"])
        )

        for page in s.get("pages_visited", []):
            pages_data.append((s["session_id"], page))

        for action in s.get("actions", []):
            actions_data.append((s["session_id"], action))

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.user_sessions
        VALUES (%s,%s,%s,%s,%s)
    """,
        sessions_data,
    )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.user_session_pages
        VALUES (%s,%s)
    """,
        pages_data,
    )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.user_session_actions
        VALUES (%s,%s)
    """,
        actions_data,
    )

    events_data = []
    event_details_data = []

    for e in db["EventLogs"].find():
        events_data.append((e["event_id"], e["timestamp"], e["event_type"]))

        details = e.get("details", {})
        event_details_data.append(
            (e["event_id"], details.get("page"), details.get("product_id"))
        )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.event_logs
        VALUES (%s,%s,%s)
    """,
        events_data,
    )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.event_log_details
        VALUES (%s,%s,%s)
    """,
        event_details_data,
    )

    tickets_data = []
    messages_data = []

    for t in db["SupportTickets"].find():
        tickets_data.append(
            (
                t["ticket_id"],
                t["user_id"],
                t["status"],
                t["issue_type"],
                t["created_at"],
                t["updated_at"],
            )
        )

        for m in t.get("messages", []):
            messages_data.append(
                (t["ticket_id"], m["sender"], m["message"], m["timestamp"])
            )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.support_tickets
        VALUES (%s,%s,%s,%s,%s,%s)
    """,
        tickets_data,
    )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.support_ticket_messages
        VALUES (%s,%s,%s,%s)
    """,
        messages_data,
    )

    recs_data = []
    rec_products_data = []

    for r in db["UserRecommendations"].find():
        recs_data.append((r["user_id"], r["last_updated"]))

        for product in r.get("recommended_products", []):
            rec_products_data.append((r["user_id"], product))

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.user_recommendations
        VALUES (%s,%s)
    """,
        recs_data,
    )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.user_recommendation_products
        VALUES (%s,%s)
    """,
        rec_products_data,
    )

    moderation_data = []
    flags_data = []

    for m in db["ModerationQueue"].find():
        moderation_data.append(
            (
                m["review_id"],
                m["user_id"],
                m["product_id"],
                m["review_text"],
                m["rating"],
                m["moderation_status"],
                m["submitted_at"],
            )
        )

        for flag in m.get("flags", []):
            flags_data.append((m["review_id"], flag))

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.moderation_queue
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """,
        moderation_data,
    )

    execute_batch(
        cur,
        """
        INSERT INTO marketplace.moderation_flags
        VALUES (%s,%s)
    """,
        flags_data,
    )

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="mongo_replication",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "mongo", "flatten"],
) as dag:

    PythonOperator(task_id="replicate_all", python_callable=replaction_cb)
