import json
import boto3
import random
import string
from datetime import datetime, timezone, timedelta
import io

# Client S3
s3_client = boto3.client('s3')

# Configuration
BUCKET_NAME = 'shopfast-ecommerce-data'
PREFIX_RAW = 'raw/'
NUM_ORDERS = 100

# Produits réalistes
PRODUCTS_CATALOG = {
    'electronics': [
        {'product_id': 'ELEC001', 'name': 'iPhone 15 Pro', 'price': 1199.99, 'category': 'Smartphones'},
        {'product_id': 'ELEC002', 'name': 'Samsung Galaxy S24', 'price': 999.99, 'category': 'Smartphones'},
        {'product_id': 'ELEC003', 'name': 'MacBook Air M3', 'price': 1499.99, 'category': 'Laptops'},
        {'product_id': 'ELEC004', 'name': 'Dell XPS 15', 'price': 1299.99, 'category': 'Laptops'},
    ],
    'clothing': [
        {'product_id': 'CLOT001', 'name': 'Nike Air Max Sneakers', 'price': 129.99, 'category': 'Shoes'},
        {'product_id': 'CLOT002', 'name': 'Adidas Running Shoes', 'price': 119.99, 'category': 'Shoes'},
    ],
}

# Villes
CITIES = ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice']

# Méthodes de paiement et statuts
PAYMENT_METHODS = ['credit_card', 'paypal', 'apple_pay']
ORDER_STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']

# ---- Fonctions utilitaires ----
def generate_order_id():
    return 'ORD-' + datetime.now(timezone.utc).strftime('%Y%m%d') + '-' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def generate_customer_id():
    return 'CUST-' + ''.join(random.choices(string.digits, k=8))

def generate_email(customer_id):
    domains = ['gmail.com', 'yahoo.fr']
    return f'customer{customer_id.split("-")[1]}@{random.choice(domains)}'

def select_products():
    num_items = random.choices([1, 2, 3], weights=[0.5, 0.3, 0.2])[0]
    all_products = [p for cat in PRODUCTS_CATALOG.values() for p in cat]
    selected = random.sample(all_products, num_items)
    items = []
    for product in selected:
        quantity = random.choices([1, 2], weights=[0.8, 0.2])[0]
        discount_percentage = random.choice([0, 5, 10]) if random.random() < 0.2 else 0
        unit_price = product['price']
        discount_amount = round(unit_price * (discount_percentage / 100), 2)
        items.append({
            'product_id': product['product_id'],
            'product_name': product['name'],
            'category': product['category'],
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_percentage': discount_percentage,
            'discount_amount': round(discount_amount * quantity, 2),
            'subtotal': round((unit_price - discount_amount) * quantity, 2)
        })
    return items

def calculate_shipping(subtotal, city):
    if subtotal >= 100:
        return 0.0
    major_cities = ['Paris', 'Lyon', 'Marseille']
    return 4.99 if city in major_cities else 7.99

def generate_ecommerce_order():
    customer_id = generate_customer_id()
    items = select_products()
    subtotal = sum(i['subtotal'] for i in items)
    total_discount = sum(i['discount_amount'] for i in items)
    city = random.choice(CITIES)
    shipping_cost = calculate_shipping(subtotal, city)
    tax_rate = 0.20
    tax_amount = round((subtotal + shipping_cost) * tax_rate, 2)
    total_amount = round(subtotal + shipping_cost + tax_amount, 2)
    status = random.choices(ORDER_STATUSES, weights=[0.15,0.4,0.25,0.15,0.05])[0]
    payment_method = random.choices(PAYMENT_METHODS, weights=[0.7,0.2,0.1])[0]
    order_time = datetime.now(timezone.utc) - timedelta(days=random.randint(0,7))
    return {
        'order_id': generate_order_id(),
        'order_date': order_time.isoformat(),
        'customer_id': customer_id,
        'customer_email': generate_email(customer_id),
        'customer_city': city,
        'items': items,
        'num_items': len(items),
        'total_quantity': sum(i['quantity'] for i in items),
        'subtotal': subtotal,
        'total_discount': total_discount,
        'shipping_cost': shipping_cost,
        'tax_rate': tax_rate,
        'tax_amount': tax_amount,
        'total_amount': total_amount,
        'payment_method': payment_method,
        'status': status,
        'created_at': datetime.now(timezone.utc).isoformat()
    }

# ---- Lambda Handler ----
def lambda_handler(event, context):
    try:
        now = datetime.now(timezone.utc)
        timestamp = now.strftime('%Y%m%d_%H%M%S')

        orders = [generate_ecommerce_order() for _ in range(NUM_ORDERS)]
        total_revenue = sum(o['total_amount'] for o in orders)
        stats = {
            'total_orders': len(orders),
            'total_revenue': round(total_revenue,2),
            'average_order_value': round(total_revenue/len(orders),2)
        }

        # Préparation JSON Lines pour Athena
        s3_key = f'{PREFIX_RAW}year={now.strftime("%Y")}/month={now.strftime("%m")}/day={now.strftime("%d")}/orders_{timestamp}.jsonl'
        with io.StringIO() as buffer:
            for o in orders:
                buffer.write(json.dumps(o)+'\n')
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/json'
            )

        return {
            'statusCode':200,
            'body':json.dumps({
                'message':'Commandes générées avec succès',
                'statistics': stats,
                's3_location': f's3://{BUCKET_NAME}/{s3_key}'
            })
        }

    except Exception as e:
        print(f"❌ Erreur : {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode':500,
            'body':json.dumps({'error':str(e)})
        }
