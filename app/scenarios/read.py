# =========================
# READ – FILTER
# =========================
def read_product_filtered_sql(conn, _):
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM products
        WHERE price > 100 AND category = 'product_category_1'
    """)
    return cur.fetchall()


def read_product_filtered_mongo(db, _):
    return list(db.products.find({
        "price": {"$gt": 100},
        "category": "product_category_1"
    }))


def read_product_filtered_redis(r, _):
    result = []
    for key in r.scan_iter("product:*"):
        data = r.hgetall(key)
        try:
            if float(data.get("price", 0)) > 100 and data.get("category") == "product_category_1":
                result.append(data)
        except:
            continue
    return result


# =========================
# READ – JOIN (sales + products)
# =========================
def read_sales_join_sql(conn, customer_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT s.sale_id, p.name, si.product_quantity
        FROM sales s
        JOIN sale_items si ON s.sale_id = si.sale_id
        JOIN products p ON si.product_id = p.product_id
        WHERE s.customer_id = %s
    """, (customer_id,))
    return cur.fetchall()


def read_sales_join_mongo(db, customer_id):
    sales = list(db.sales.find({"customer_id": customer_id}))
    result = []

    for s in sales:
        items = list(db.sale_items.find({"sale_id": s["_id"]}))
        for it in items:
            product = db.products.find_one({"_id": it["product_id"]})
            result.append({
                "sale_id": s["_id"],
                "product": product,
                "quantity": it["product_quantity"]
            })
    return result


def read_sales_join_redis(r, customer_id):
    result = []
    for key in r.scan_iter("sale:*"):
        if key.endswith(":products"):
            continue

        sale = r.hgetall(key)
        if sale.get("customer_id") == str(customer_id):
            sid = key.split(":")[1]

            for item_json in r.lrange(f"sale:{sid}:products", 0, -1):
                import json
                it = json.loads(item_json)
                product = r.hgetall(f"product:{it.get('id')}")
                result.append({
                    "sale_id": sid,
                    "product": product,
                    "quantity": it.get("product_quantity", 1)
                })
    return result


# =========================
# READ – AGGREGATION
# =========================
def read_aggregation_sql(conn, _):
    cur = conn.cursor()
    cur.execute("""
        SELECT category, COUNT(*), AVG(price)
        FROM products
        GROUP BY category
    """)
    return cur.fetchall()


def read_aggregation_mongo(db, _):
    return list(db.products.aggregate([
        {
            "$group": {
                "_id": "$category",
                "count": {"$sum": 1},
                "avg_price": {"$avg": "$price"}
            }
        }
    ]))


def read_aggregation_redis(r, _):
    from collections import defaultdict

    groups = defaultdict(list)

    for key in r.scan_iter("product:*"):
        data = r.hgetall(key)
        category = data.get("category")
        try:
            price = float(data.get("price", 0))
            groups[category].append(price)
        except:
            continue

    result = []
    for cat, prices in groups.items():
        result.append({
            "category": cat,
            "count": len(prices),
            "avg_price": sum(prices)/len(prices) if prices else 0
        })

    return result