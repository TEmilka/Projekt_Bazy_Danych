import json

import redis
import os

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)

# ---------- CUSTOMERS ----------
r.hset("customer:1", mapping={
    "name": "Jan",
    "surname": "Kowalski",
    "pesel": "80051012345",
    "phone": "123456789"
})

r.hset("customer:2", mapping={
    "name": "Anna",
    "surname": "Nowak",
    "pesel": "90120154321",
    "phone": "987654321"
})

# ---------- PRODUCTS ----------
r.hset("product:1", mapping={
    "name": "Paracetamol",
    "is_prescription_required": "False",
    "barcode": "123456",
    "price": "5.5",
    "category": "Painkiller"
})

r.hset("product:2", mapping={
    "name": "Amoxicillin",
    "is_prescription_required": "True",
    "barcode": "234567",
    "price": "12.0",
    "category": "Antibiotic"
})

# ---------- EMPLOYEES ----------
r.hset("employee:1", mapping={
    "name": "Marek",
    "surname": "Nowak",
    "phone": "555111222",
    "job_position": "Pharmacist",
    "salary": "4500"
})

r.hset("employee:2", mapping={
    "name": "Ewa",
    "surname": "Kowalska",
    "phone": "555333444",
    "job_position": "Cashier",
    "salary": "3000"
})

# ---------- SALES ----------
r.hset("sale:1", mapping={
    "customer_id": "1",
    "employee_id": "1",
    "sale_date": "2026-03-01T10:00:00",
    "total_value": "17.5",
    "status": "Completed"
})

item = {
    "product_id": 1,
    "quantity": 2,
    "price": 5.5
}

item2 = {
    "product_id": 2,
    "quantity": 1,
    "price": 12.0
}

r.rpush("sale:1:products", json.dumps(item))
r.rpush("sale:1:products", json.dumps(item2))

# ---------- PRESCRIPTIONS -----------
r.hset("prescription:1", mapping={
    "customer_id": "2",
    "issue_date": "2026-03-02T09:00:00",
    "expiry_date": "2026-04-02T09:00:00",
    "status": "Active"
})
