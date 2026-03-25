# 1 pracownik
def delete_employee_sql(conn,id):
    cur=conn.cursor()
    cur.execute("DELETE FROM employees WHERE employee_id=%s",(id,))
    conn.commit()

def delete_employee_mongo(db,id):
    db.employees.delete_one({"_id":id})

def delete_employee_redis(r,id):
    r.delete(f"employee:{id}")


# 2 recepta
def delete_prescription_sql(conn,id):
    cur=conn.cursor()
    cur.execute("DELETE FROM prescriptions WHERE prescription_id=%s",(id,))
    conn.commit()

def delete_prescription_mongo(db,id):
    db.prescriptions.delete_one({"_id":id})

def delete_prescription_redis(r,id):
    r.delete(f"prescription:{id}")


# 3 klient + zależności
def delete_customer_sql(conn,id):
    cur=conn.cursor()
    cur.execute("DELETE FROM customers WHERE customer_id=%s",(id,))
    conn.commit()

def delete_customer_mongo(db,id):
    db.customers.delete_one({"_id":id})

def delete_customer_redis(r,id):
    r.delete(f"customer:{id}")