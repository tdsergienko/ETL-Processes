import random
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker

fake = Faker()

client = MongoClient("mongodb://airflow:airflow@mongo:27017/?authSource=admin")
db = client["airflow"]

user_sessions = db["UserSessions"]
event_logs = db["EventLogs"]
support_tickets = db["SupportTickets"]
user_recommendations = db["UserRecommendations"]
moderation_queue = db["ModerationQueue"]


def random_timestamp():
    start = datetime.utcnow() - timedelta(days=30)
    return start + timedelta(minutes=random.randint(0, 60 * 24 * 30))


def generate_user_sessions(n=100):
    sessions = []
    for _ in range(n):
        start = random_timestamp()
        end = start + timedelta(minutes=random.randint(5, 120))
        sessions.append(
            {
                "session_id": f"sess_{uuid.uuid4().hex[:8]}",
                "user_id": f"user_{random.randint(1, 50)}",
                "start_time": start,
                "end_time": end,
                "pages_visited": random.sample(
                    ["/home", "/products", "/cart", "/checkout", "/profile"],
                    k=random.randint(2, 5),
                ),
                "device": random.choice(["mobile", "desktop", "tablet"]),
                "actions": random.sample(
                    ["login", "view_product", "add_to_cart", "checkout", "logout"],
                    k=random.randint(2, 5),
                ),
            }
        )
    user_sessions.insert_many(sessions)


def generate_event_logs(n=300):
    events = []
    for _ in range(n):
        events.append(
            {
                "event_id": f"evt_{uuid.uuid4().hex[:8]}",
                "timestamp": random_timestamp(),
                "event_type": random.choice(["click", "view", "purchase", "error"]),
                "details": {
                    "page": random.choice(["/home", "/products", "/cart"]),
                    "product_id": f"prod_{random.randint(1, 200)}",
                },
            }
        )
    event_logs.insert_many(events)


def generate_support_tickets(n=50):
    tickets = []
    for _ in range(n):
        created = random_timestamp()
        updated = created + timedelta(hours=random.randint(1, 48))
        tickets.append(
            {
                "ticket_id": f"ticket_{uuid.uuid4().hex[:6]}",
                "user_id": f"user_{random.randint(1, 50)}",
                "status": random.choice(["open", "closed", "in_progress"]),
                "issue_type": random.choice(["payment", "delivery", "technical"]),
                "messages": [
                    {
                        "sender": "user",
                        "message": fake.sentence(),
                        "timestamp": created,
                    },
                    {
                        "sender": "support",
                        "message": fake.sentence(),
                        "timestamp": updated,
                    },
                ],
                "created_at": created,
                "updated_at": updated,
            }
        )
    support_tickets.insert_many(tickets)


def generate_user_recommendations(n=50):
    recs = []
    for user_id in range(1, n + 1):
        recs.append(
            {
                "user_id": f"user_{user_id}",
                "recommended_products": [
                    f"prod_{random.randint(1, 200)}" for _ in range(5)
                ],
                "last_updated": random_timestamp(),
            }
        )
    user_recommendations.insert_many(recs)


def generate_moderation_queue(n=80):
    reviews = []
    for _ in range(n):
        reviews.append(
            {
                "review_id": f"rev_{uuid.uuid4().hex[:6]}",
                "user_id": f"user_{random.randint(1, 50)}",
                "product_id": f"prod_{random.randint(1, 200)}",
                "review_text": fake.sentence(),
                "rating": random.randint(1, 5),
                "moderation_status": random.choice(["pending", "approved", "rejected"]),
                "flags": random.sample(
                    ["contains_images", "possible_spam", "offensive_language"],
                    k=random.randint(0, 2),
                ),
                "submitted_at": random_timestamp(),
            }
        )
    moderation_queue.insert_many(reviews)


if __name__ == "__main__":
    print("Generating fake Mongo data...")
    generate_user_sessions()
    generate_event_logs()
    generate_support_tickets()
    generate_user_recommendations()
    generate_moderation_queue()
    print("Done.")
