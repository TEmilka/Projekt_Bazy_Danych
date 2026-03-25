# 1. telefon
def update_phone_sql(conn,d):
    cur=conn.cursor()
    cur.execute("UPDATE customers SET phone=%s WHERE customer_id=%s",(d["phone"],d["id"]))
    conn.commit()

def update_phone_mongo(db,d):
    db.customers.update_one({"_id":d["id"]},{"$set":{"phone":d["phone"]}})

def update_phone_redis(r,d):
    r.hset(f"customer:{d['id']}","phone",d["phone"])


# 2. awans
def promote_sql(conn,d):
    cur=conn.cursor()
    cur.execute("UPDATE employees SET job_position=%s,salary=%s WHERE employee_id=%s",
                (d["job_position"],d["salary"],d["id"]))
    conn.commit()

def promote_mongo(db,d):
    db.employees.update_one({"_id":d["id"]},{"$set":d})

def promote_redis(r,d):
    r.hset(f"employee:{d['id']}",mapping=d)


# 3. wygasłe recepty
from datetime import datetime

def expire_sql(conn,_):
    cur=conn.cursor()
    cur.execute("UPDATE prescriptions SET status='Expired' WHERE expiry_date < NOW()")
    conn.commit()

def expire_mongo(db,_):
    db.prescriptions.update_many({"expiry_date":{"$lt":datetime.now()}},{"$set":{"status":"Expired"}})

def expire_redis(r,_):
    pass