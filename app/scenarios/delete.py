# =========================
# DELETE – SALES BY STATUS
# =========================
from datetime import datetime, timedelta


def delete_sales_by_status_sql(conn, _):
    cur = conn.cursor()
    cur.execute("DELETE FROM sales WHERE status = 'Closed'")
    conn.commit()


def delete_sales_by_status_mongo(db, _):
    db.sales.delete_many({"status": "Closed"})


def delete_sales_by_status_redis(r, _):
    for key in r.scan_iter("sale:*"):
        if key.endswith(":products"):
            continue

        data = r.hgetall(key)
        if data.get("status") == "Closed":
            sid = key.split(":")[1]
            r.delete(key)
            r.delete(f"sale:{sid}:products")


# =========================
# DELETE – HIGH SALARY EMPLOYEES
# =========================

def delete_high_salary_employees_sql(conn, _):
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM employees e
        WHERE salary > 7000
        AND NOT EXISTS (
            SELECT 1 FROM sales s WHERE s.employee_id = e.employee_id
        )
    """)

    conn.commit()


def delete_high_salary_employees_mongo(db, _):
    db.employees.delete_many({"salary": {"$gt": 7000}})


def delete_high_salary_employees_redis(r, _):
    for key in r.scan_iter("employee:*"):
        salary = r.hget(key, "salary")
        try:
            if float(salary) > 7000:
                r.delete(key)
        except:
            continue


# =========================
# 3. DELETE OLD SALES
# =========================

def delete_old_sales_postgres(conn, _):
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM sales
        WHERE sale_date < NOW() - INTERVAL '30 days'
    """)

    conn.commit()

def delete_old_sales_mysql(conn, _):
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM sales
        WHERE sale_date < NOW() - INTERVAL 30 DAY
    """)

    conn.commit()


def delete_old_sales_mongo(db, _):
    cutoff = datetime.now() - timedelta(days=30)

    old_sales = db.sales.find({"sale_date": {"$lt": cutoff}})

    for s in old_sales:
        db.sale_items.delete_many({"sale_id": s["_id"]})

    db.sales.delete_many({"sale_date": {"$lt": cutoff}})


def delete_old_sales_redis(r, _):
    cutoff = datetime.now() - timedelta(days=30)

    for key in r.scan_iter("sale:*"):
        if key.endswith(":products"):
            continue

        data = r.hgetall(key)

        if "sale_date" not in data:
            continue

        try:
            sale_date = datetime.fromisoformat(data["sale_date"])
        except:
            continue

        if sale_date < cutoff:
            sid = key.split(":")[1]

            r.delete(key)
            r.delete(f"sale:{sid}:products")