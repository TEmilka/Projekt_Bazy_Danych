# 1 pracownik
def delete_employee_sql(conn,id):
    cur=conn.cursor()
    cur.execute("DELETE FROM employees WHERE employee_id=%s",(id,))
    conn.commit()

def delete_employee_mongo(db,id):
    db.employees.delete_one({"_id":id})

def delete_employee_redis(r,id):
    r.delete(f"employee:{id}")


# 2 recepta - delete by id
def delete_prescription_sql(conn,id):
    cur=conn.cursor()
    cur.execute("DELETE FROM prescriptions WHERE prescription_id=%s",(id,))
    conn.commit()

def delete_prescription_mongo(db,id):
    db.prescriptions.delete_one({"_id":id})

def delete_prescription_redis(r,id):
    r.delete(f"prescription:{id}")

# 2b recepta - delete all with status Closed
def delete_closed_prescriptions_sql(conn):
    cur=conn.cursor()
    cur.execute("DELETE FROM prescriptions WHERE status=%s", ("Closed",))
    conn.commit()

def delete_closed_prescriptions_mongo(db):
    db.prescriptions.delete_many({"status":"Closed"})

def delete_closed_prescriptions_redis(r):
    for key in r.keys("prescription:*"):
        st = r.hget(key, "status")
        if st == "Closed":
            r.delete(key)


# 3 klient + zależności
def delete_customer_sql(conn,id):
    cur=conn.cursor()
    # delete sales for customer (sale_items cascade on sale deletion)
    cur.execute("DELETE FROM sales WHERE customer_id=%s", (id,))
    # delete prescriptions for customer
    cur.execute("DELETE FROM prescriptions WHERE customer_id=%s", (id,))
    # finally delete customer
    cur.execute("DELETE FROM customers WHERE customer_id=%s",(id,))
    conn.commit()

def delete_customer_mongo(db,id):
    db.sales.delete_many({"customer_id":id})
    db.prescriptions.delete_many({"customer_id":id})
    db.customers.delete_one({"_id":id})

def delete_customer_redis(r,id):
    # delete customer hash
    r.delete(f"customer:{id}")
    # delete prescriptions for customer
    for key in r.keys("prescription:*"):
        if r.hget(key, "customer_id") == str(id):
            r.delete(key)
    # delete sales for customer (sale hash + products list)
    for key in r.keys("sale:*"):
        # skip sale product lists
        if key.endswith(":products"):
            continue
        if r.hget(key, "customer_id") == str(id):
            sid = key.split(":")[1]
            r.delete(key)
            r.delete(f"sale:{sid}:products")