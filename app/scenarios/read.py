def read_product_sql(conn, d):
    cur = conn.cursor()
    cur.execute("SELECT * FROM products WHERE name=%s", (d["name"],))
    return cur.fetchone()


def read_product_mongo(db, d):
    return db.products.find_one({"name": d["name"]})


def read_product_redis(r, d):
    # szybkie pobieranie zamiast scanowania całej bazy
    try:
        idx = int(d["name"].split(" ")[1])
        return r.hgetall(f"product:{idx}")
    except:
        return None


# =========================
# SALES
# =========================
def read_sales_sql(conn, sale_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.sale_id, s.customer_id, s.employee_id, s.sale_date, s.total_value, s.status,
               si.product_id, si.product_quantity, p.name, p.price
        FROM sales s
        JOIN sale_items si ON s.sale_id = si.sale_id
        LEFT JOIN products p ON si.product_id = p.product_id
        WHERE s.sale_id = %s
        """,
        (sale_id,)
    )
    return cur.fetchall()


def read_sales_mongo(db, sale_id):
    sale = db.sales.find_one({"_id": sale_id})
    if not sale:
        return None

    items = list(db.sale_items.find({"sale_id": sale_id}))

    for it in items:
        product = db.products.find_one({"_id": it["product_id"]})
        it["product"] = product

    sale["items"] = items
    return sale


def read_sales_redis(r, sale_id):
    import json

    sale_key = f"sale:{sale_id}"

    if not r.exists(sale_key):
        return None

    sale = r.hgetall(sale_key)

    items = []
    for item_json in r.lrange(f"sale:{sale_id}:products", 0, -1):
        it = json.loads(item_json)
        product = r.hgetall(f"product:{it.get('product_id')}")
        it["product"] = product
        items.append(it)

    sale["items"] = items
    return sale


# =========================
# PRESCRIPTIONS
# =========================
def read_prescriptions_sql(conn, customer_id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM prescriptions WHERE customer_id=%s", (customer_id,))
    return cur.fetchall()


def read_prescriptions_mongo(db, customer_id):
    return list(db.prescriptions.find({"customer_id": customer_id}))


def read_prescriptions_redis(r, customer_id):
    result = []

    for key in r.scan_iter("prescription:*"):
        data = r.hgetall(key)
        if data.get("customer_id") == str(customer_id):
            result.append(data)

    return result