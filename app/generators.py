import random
import datetime

def product(i):
    return {
        "id": i,
        "name": f"Product {i}",
        "is_prescription_required": bool(i % 2),
        "barcode": i,
        "price": random.randint(10, 500),
        "category": f"cat_{i % 10}"
    }

def update_price(i):
    return {
        "id": i,
        "price": random.randint(10, 500)
    }

def sale(i, size):
    return {
        "id": i,
        "customer_id": random.randrange(0, size),
        "employee_id": random.randrange(0, size),
        "products": [product(i*10 + j) for j in range(5)],
        "sale_date": datetime.datetime.now(),
        "total_value": random.randint(100, 1000),
        "status": "Closed" if random.random() < 0.35 else "Open"
    }

def customer(i):
    return {
        "id": i,
        "name": f"Customer {i}",
        "surname": f"Surname {i}",
        "pesel": f"{80000000000 + i}",
        "phone": f"{600000000 + (i % 1000000)}"
    }

def employee(i):
    return {
        "id": i,
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
        "id": i,
        "customer_id": random.randrange(0, size),
        "issue_date": issue,
        "expiry_date": expiry,
        "status": "Open" if random.random() < 0.75 else "Closed"
    }
def update_phone(i):
    return {"id": i, "phone": f"{700000000 + (i % 1000000)}"}