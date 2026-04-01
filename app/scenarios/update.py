# =========================
# UPDATE – PRICE BY CATEGORY
# =========================
def update_price_by_category_sql(conn, _):
    cur = conn.cursor()
    cur.execute("""
        UPDATE products
        SET price = price * 1.1
        WHERE category = 'product_category_1'
    """)
    conn.commit()


def update_price_by_category_mongo(db, _):
    db.products.update_many(
        {"category": "product_category_1"},
        {"$mul": {"price": 1.1}}
    )


def update_price_by_category_redis(r, _):
    for key in r.scan_iter("product:*"):
        data = r.hgetall(key)
        if data.get("category") == "product_category_1":
            try:
                price = float(data.get("price", 0)) * 1.1
                r.hset(key, "price", str(price))
            except:
                continue

# =========================
# UPDATE – PROMOTE EMPLOYEE
# =========================
def promote_sql(conn,d):
    cur=conn.cursor()
    cur.execute("UPDATE employees SET job_position=%s,salary=%s WHERE employee_id=%s",
                (d["job_position"],d["salary"],d["id"]))
    conn.commit()

def promote_mongo(db,d):
    db.employees.update_one({"_id":d["id"]},{"$set":d})

def promote_redis(r,d):
    r.hset(f"employee:{d['id']}",mapping=d)


# =========================
# UPDATE – EXPIRE PRESCRIPTIONS
# =========================
from datetime import datetime

def expire_sql(conn,_):
    cur=conn.cursor()
    cur.execute("UPDATE prescriptions SET status='Expired' WHERE expiry_date < NOW()")
    conn.commit()

def expire_mongo(db,_):
    db.prescriptions.update_many({"expiry_date":{"$lt":datetime.now()}},{"$set":{"status":"Expired"}})

def expire_redis(r, _):
    now = datetime.now()

    for key in r.scan_iter("prescription:*"):
        data = r.hgetall(key)

        if "expiry_date" not in data:
            continue

        expiry = datetime.fromisoformat(data["expiry_date"])

        if expiry < now:
            r.hset(key, "status", "Expired")