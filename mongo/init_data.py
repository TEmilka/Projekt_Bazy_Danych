import os
from pymongo import MongoClient
from app.generators import product, customer, employee, prescription, sale

def populate(size, host=None):
    host = host or os.getenv('MONGO_HOST', 'localhost')
    client = MongoClient(f"mongodb://admin:admin@{host}:27017/")
    db = client['pharmacy_db']

    for col in ['customers','products','employees','prescriptions','sales','sale_items']:
        if col in db.list_collection_names():
            db[col].drop()

    batch = 1000
    docs = []
    for i in range(size):
        docs.append({
            '_id': customer(i)['id'],
            'name': customer(i)['name'],
            'surname': customer(i)['surname'],
            'pesel': customer(i)['pesel'],
            'phone': customer(i)['phone']
        })
        if len(docs) >= batch:
            db.customers.insert_many(docs)
            docs = []
    if docs:
        db.customers.insert_many(docs)

    docs = []
    for i in range(size):
        p = product(i)
        docs.append({'_id': p['id'], 'name': p['name'], 'is_prescription_required': p['is_prescription_required'], 'barcode': p['barcode'], 'price': p['price'], 'category': p['category']})
        if len(docs) >= batch:
            db.products.insert_many(docs)
            docs = []
    if docs:
        db.products.insert_many(docs)

    docs = []
    for i in range(size):
        e = employee(i)
        docs.append({'_id': e['id'], 'name': e['name'], 'surname': e['surname'], 'phone': e['phone'], 'job_position': e['job_position'], 'salary': e['salary']})
        if len(docs) >= batch:
            db.employees.insert_many(docs)
            docs = []
    if docs:
        db.employees.insert_many(docs)

    docs = []
    for i in range(size):
        pr = prescription(i)
        docs.append({'_id': pr['id'], 'customer_id': pr['customer_id'], 'issue_date': pr['issue_date'], 'expiry_date': pr['expiry_date'], 'status': pr['status']})
        if len(docs) >= batch:
            db.prescriptions.insert_many(docs)
            docs = []
    if docs:
        db.prescriptions.insert_many(docs)

    docs_sales = []
    docs_items = []
    for i in range(size):
        s = sale(i)
        docs_sales.append({'_id': s['id'], 'customer_id': s['customer_id'], 'employee_id': s['employee_id'], 'sale_date': s['sale_date'], 'total_value': s['total_value'], 'status': s['status']})
        for it in s['products']:
            docs_items.append({'sale_id': s['id'], 'product_id': it['id'], 'product_quantity': it.get('product_quantity',1)})
        if len(docs_sales) >= batch:
            db.sales.insert_many(docs_sales)
            db.sale_items.insert_many(docs_items)
            docs_sales = []
            docs_items = []
    if docs_sales:
        db.sales.insert_many(docs_sales)
    if docs_items:
        db.sale_items.insert_many(docs_items)

    print(f"Populated mongo with {size} items per collection")

if __name__ == '__main__':
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    populate(n)
