db = db.getSiblingDB("apteka");

// ======================
// CUSTOMERS
// ======================
db.createCollection("customers");
db.customers.insertMany([
  { _id: 1, name: "Jan", surname: "Kowalski", pesel: 80051012345, phone: 123456789 },
  { _id: 2, name: "Anna", surname: "Nowak", pesel: 90120154321, phone: 987654321 }
]);


// ======================
// PRODUCTS
// ======================
db.createCollection("products");
db.products.insertMany([
  { _id: 1, name: "Paracetamol", is_prescription_required: false, barcode: 123456, price: 5.5, category: "Painkiller" },
  { _id: 2, name: "Amoxicillin", is_prescription_required: true, barcode: 234567, price: 12.0, category: "Antibiotic" }
]);


// ======================
// EMPLOYEES
// ======================
db.createCollection("employees");
// Kolekcja pracowników
db.employees.insertMany([
  { _id: 1, name: "Marek", surname: "Nowak", phone: 555111222, job_position: "Pharmacist", salary: 4500 },
  { _id: 2, name: "Ewa", surname: "Kowalska", phone: 555333444, job_position: "Cashier", salary: 3000 }
]);

// ======================
// SALES
// ======================
db.createCollection("sales");
db.sales.insertMany([
  { _id: 1, customer_id: 1, employee_id: 1, sale_date: ISODate("2026-03-01T10:00:00Z"), total_value: 17.5, status: "Completed" }
])

// ======================
// SALE ITEMS
// ======================
db.createCollection("sale_items");
db.sale_items.insertMany([
  { _id: 1, sale_id: 1, product_id: 1, product_quantity: 2 },
  { _id: 2, sale_id: 1, product_id: 2, product_quantity: 1 }
]);

// ======================
// PRESCRIPTIONS
// ======================
db.createCollection("prescriptions");
db.prescriptions.insertMany([
  { _id: 1, customer_id: 2, issue_date: ISODate("2026-03-02T09:00:00Z"), expiry_date: ISODate("2026-04-02T09:00:00Z"), status: "Active" }
]);