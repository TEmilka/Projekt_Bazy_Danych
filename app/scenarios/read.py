def read_product_sql(conn,d):
    cur=conn.cursor()
    cur.execute("SELECT * FROM products WHERE name=%s",(d["name"],))
    return cur.fetchone()

def read_product_mongo(db,d):
    return db.products.find_one({"name":d["name"]})

def read_product_redis(r,d):
    for key in r.keys("product:*"):
        if r.hget(key,"name")==d["name"]:
            return r.hgetall(key)


def read_sales_sql(conn,sale_id):
    cur=conn.cursor()
    # fetch sale and its items plus product info
    cur.execute(
        """
        SELECT s.sale_id, s.customer_id, s.employee_id, s.sale_date, s.total_value, s.status,
               si.product_id, si.product_quantity, p.name AS product_name, p.is_prescription_required, p.price
        FROM sales s
        JOIN sale_items si ON s.sale_id = si.sale_id
        LEFT JOIN products p ON si.product_id = p.product_id
        WHERE s.sale_id = %s
        """,
        (sale_id,)
    )
    rows = cur.fetchall()
    return rows

def read_sales_mongo(db,sale_id):
    sale = db.sales.find_one({"_id": sale_id})
    if not sale:
        return None
    items = list(db.sale_items.find({"sale_id": sale_id}))
    # attach product info for each item if available
    for it in items:
        prod = db.products.find_one({"_id": it.get("product_id")})
        it["product"] = prod
    sale["items"] = items
    return sale

def read_sales_redis(r,sale_id):
    import json
    sale_key = f"sale:{sale_id}"
    if not r.exists(sale_key):
        return None
    sale = r.hgetall(sale_key)
    items = []
    for v in r.lrange(f"sale:{sale_id}:products", 0, -1):
        it = json.loads(v)
        prod = r.hgetall(f"product:{it.get('product_id')}")
        it["product"] = prod
        items.append(it)
    sale["items"] = items
    return sale


def read_prescriptions_sql(conn,id):
    cur=conn.cursor()
    cur.execute("SELECT * FROM prescriptions WHERE customer_id=%s",(id,))
    return cur.fetchall()

def read_prescriptions_mongo(db,id):
    return list(db.prescriptions.find({"customer_id":id}))

def read_prescriptions_redis(r,id):
    return r.keys(f"prescription:*")