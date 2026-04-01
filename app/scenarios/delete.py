# =========================
# 1. DELETE EMPLOYEE
# =========================

def delete_employee_sql(conn, id):
    cur = conn.cursor()

    # 🔥 najpierw usuń sales (bo FK)
    cur.execute("DELETE FROM sales WHERE employee_id=%s", (id,))

    # potem employee
    cur.execute("DELETE FROM employees WHERE employee_id=%s", (id,))

    conn.commit()


def delete_employee_mongo(db, id):
    db.sales.delete_many({"employee_id": id})
    db.employees.delete_one({"_id": id})


def delete_employee_redis(r, id):
    # usuń employee
    r.delete(f"employee:{id}")

    # usuń sales powiązane
    for key in r.scan_iter("sale:*"):
        if key.endswith(":products"):
            continue

        if r.hget(key, "employee_id") == str(id):
            sid = key.split(":")[1]
            r.delete(key)
            r.delete(f"sale:{sid}:products")


# =========================
# 2. DELETE PRESCRIPTION
# =========================

def delete_prescription_sql(conn, id):
    cur = conn.cursor()
    cur.execute("DELETE FROM prescriptions WHERE prescription_id=%s", (id,))
    conn.commit()


def delete_prescription_mongo(db, id):
    db.prescriptions.delete_one({"_id": id})


def delete_prescription_redis(r, id):
    r.delete(f"prescription:{id}")


# =========================
# 3. DELETE CUSTOMER (zależności)
# =========================

def delete_customer_sql(conn, id):
    cur = conn.cursor()

    cur.execute("DELETE FROM sales WHERE customer_id=%s", (id,))
    cur.execute("DELETE FROM prescriptions WHERE customer_id=%s", (id,))
    cur.execute("DELETE FROM customers WHERE customer_id=%s", (id,))

    conn.commit()


def delete_customer_mongo(db, id):
    db.sales.delete_many({"customer_id": id})
    db.prescriptions.delete_many({"customer_id": id})
    db.customers.delete_one({"_id": id})


def delete_customer_redis(r, id):
    r.delete(f"customer:{id}")

    for key in r.scan_iter("prescription:*"):
        if r.hget(key, "customer_id") == str(id):
            r.delete(key)

    for key in r.scan_iter("sale:*"):
        if key.endswith(":products"):
            continue

        if r.hget(key, "customer_id") == str(id):
            sid = key.split(":")[1]
            r.delete(key)
            r.delete(f"sale:{sid}:products")