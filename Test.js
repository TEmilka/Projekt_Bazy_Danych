employees: [
    {
        "_id": Int32,
        "name": String,
        "surname": String,
        "phone": Int32,
        "job_posotion": String,
        "salary": Int32
    }
]
customers: [
    {
        "_id": Int32,
        "name": String,
        "surname": String,
        "pesel": Double,
        "phone": Int32
    }
]
prescriptions: [
    {
        "_id": Int32,
        "customer_id": Int32,
        "issue_date": Date,
        "expiry_date": Date,
        "status": String
    }
]
products: [
    {
        "_id": Int32,
        "name": String,
        "is_prescription_required": Boolean,
        "barcode": Int32,
        "price": Int32,
        "category": String
    }
]
sales: [
    {
        "_id": Int32,
        "customer_id": Int32,
        "employee_id": Int32,
        "products": [
            {
                "_id": Int32,
                "name": String,
                "is_prescription_required": Boolean,
                "barcode": Int32,
                "price": Int32,
                "category": String,
                "product_quantity": Int32
            },
            {
                "_id": Int32,
                "name": String,
                "is_prescription_required": Boolean,
                "barcode": Int32,
                "price": Int32,
                "category": String,
                "product_quantity": Int32
            }
        ],
        "sale_date": Date,
        "total_value": Double,
        "status": String
    }
]




