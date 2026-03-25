import random
from datetime import datetime, timedelta

def product(i):
    return {
        "id": i,
        "name": f"Product{i}",
        "is_prescription_required": random.choice([True, False]),
        "barcode": str(100000+i),
        "price": round(random.uniform(5,100),2),
        "category": "General"
    }

def customer(i):
    return {
        "id": i,
        "name": f"Name{i}",
        "surname": f"Surname{i}",
        "pesel": str(80000000000+i),
        "phone": "123456789"
    }

def employee(i):
    return {
        "id": i,
        "name": f"Emp{i}",
        "surname": "Test",
        "phone": "123",
        "job_position": "Junior",
        "salary": 3000
    }

def prescription(i):
    now = datetime.now()
    return {
        "id": i,
        "customer_id": i,
        "issue_date": now,
        "expiry_date": now - timedelta(days=1),
        "status": "Active"
    }

def sale(i):
    return {
        "id": i,
        "customer_id": i,
        "employee_id": i,
        "total_value": 100,
        "status": "Completed"
    }

def update_phone(i):
    return {"id": i, "phone": "999999999"}

def promote(i):
    return {"id": i, "job_position": "Senior", "salary": 6000}

def read_name(i):
    return {"name": f"Product{i}"}