import random
import datetime

def product(i):
    return {
        "id": i + 1,  # ✅ FIX (Mongo/Redis też będą miały 1..N)
        "name": f"Product {i}",
        "is_prescription_required": bool(i % 2),
        "barcode": i,
        "price": random.randint(10, 500),
        "category": f"cat_{i % 10}"
    }

def update_price(i):
    return {
        "id": i + 1,
        "price": random.randint(10, 500)
    }

def sale(i, size):
    return {
        "id": i + 1,
        "customer_id": random.randint(1, size),   # ✅ FIX
        "employee_id": random.randint(1, size),   # ✅ FIX
        "products": [
            {
                "id": random.randint(1, size),   # ✅ FIX (było 0!)
                "product_quantity": 1
            }
            for _ in range(5)
        ],
        "sale_date": datetime.datetime.now(),
        "total_value": random.randint(100, 1000),
        "status": "Closed" if random.random() < 0.35 else "Open"
    }

def customer(i):
    return {
        "id": i + 1,  # ✅ FIX
        "name": f"Customer {i}",
        "surname": f"Surname {i}",
        "pesel": f"{80000000000 + i}",
        "phone": f"{600000000 + (i % 1000000)}"
    }

def employee(i):
    return {
        "id": i + 1,  # ✅ FIX
        "name": f"Employee {i}",
        "surname": f"EmpSurname {i}",
        "phone": f"{500000000 + (i % 1000000)}",
        "job_position": "Staff",
        "salary": random.randint(2500,8000)
    }

def prescription(i, size):
    issue = datetime.datetime.now() - datetime.timedelta(days=random.randint(0,30))
    expiry = issue + datetime.timedelta(days=30)

    return {
        "id": i + 1,
        "customer_id": random.randint(1, size),  # ✅ FIX
        "issue_date": issue,
        "expiry_date": expiry,
        "status": "Open" if random.random() < 0.75 else "Closed"
    }

def update_phone(i):
    return {"id": i + 1, "phone": f"{700000000 + (i % 1000000)}"}