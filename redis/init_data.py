import os
import json
import redis
from app.generators import product, customer, employee, prescription, sale

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=6379,
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)


def populate(size):
    # flush existing (be careful!)
    r.flushdb()
    batch = 10000

    for i in range(size):
        c = customer(i)
        r.hset(f"customer:{c['id']}", mapping={
            'name': c['name'], 'surname': c['surname'], 'pesel': c['pesel'], 'phone': c['phone']
        })
        p = product(i)
        r.hset(f"product:{p['id']}", mapping={
            'name': p['name'], 'is_prescription_required': str(p['is_prescription_required']), 'barcode': str(p['barcode']), 'price': str(p['price']), 'category': p['category']
        })
        e = employee(i)
        r.hset(f"employee:{e['id']}", mapping={
            'name': e['name'], 'surname': e['surname'], 'phone': e['phone'], 'job_position': e['job_position'], 'salary': str(e['salary'])
        })
        pr = prescription(i)
        r.hset(f"prescription:{pr['id']}", mapping={
            'customer_id': str(pr['customer_id']), 'issue_date': pr['issue_date'].isoformat(), 'expiry_date': pr['expiry_date'].isoformat(), 'status': pr['status']
        })
        s = sale(i)
        r.hset(f"sale:{s['id']}", mapping={
            'customer_id': str(s['customer_id']), 'employee_id': str(s['employee_id']), 'sale_date': s['sale_date'].isoformat(), 'total_value': str(s['total_value']), 'status': s['status']
        })
        for it in s['products']:
            r.rpush(f"sale:{s['id']}:products", json.dumps({'product_id': it['id'], 'quantity': it.get('product_quantity',1), 'price': it.get('price',None)}))

    print(f"Populated redis with {size} items per logical collection")

if __name__ == '__main__':
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    populate(n)
