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


def read_sales_sql(conn,_):
    cur=conn.cursor()
    cur.execute("SELECT * FROM sales")
    return cur.fetchall()

def read_sales_mongo(db,_):
    return list(db.sales.find())

def read_sales_redis(r,_):
    return r.keys("sale:*")


def read_prescriptions_sql(conn,id):
    cur=conn.cursor()
    cur.execute("SELECT * FROM prescriptions WHERE customer_id=%s",(id,))
    return cur.fetchall()

def read_prescriptions_mongo(db,id):
    return list(db.prescriptions.find({"customer_id":id}))

def read_prescriptions_redis(r,id):
    return r.keys(f"prescription:*")