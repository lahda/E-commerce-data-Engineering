import json
import boto3
from datetime import datetime
from urllib.parse import unquote_plus
import io

s3_client = boto3.client('s3')

# Configuration
BUCKET_NAME = 'shopfast-ecommerce-data'
PREFIX_PROCESSED = 'processed/'

# Seuils business
HIGH_VALUE_ORDER = 500
BULK_ORDER = 10


def classify_order_value(total_amount):
    if total_amount >= 1000:
        return 'premium'
    elif total_amount >= 500:
        return 'high_value'
    elif total_amount >= 100:
        return 'medium_value'
    else:
        return 'low_value'


def classify_customer_segment(total_quantity, total_amount):
    if total_quantity >= BULK_ORDER or total_amount >= 1000:
        return 'vip'
    elif total_amount >= 300:
        return 'regular'
    else:
        return 'occasional'


def calculate_profit_margin(items):
    total_cost = sum(item['unit_price'] * 0.6 * item['quantity'] for item in items)
    total_revenue = sum(item['subtotal'] for item in items)
    profit = total_revenue - total_cost
    margin_percentage = (profit / total_revenue * 100) if total_revenue > 0 else 0
    return {
        'estimated_cost': round(total_cost, 2),
        'revenue': round(total_revenue, 2),
        'profit': round(profit, 2),
        'margin_percentage': round(margin_percentage, 2)
    }


def analyze_product_mix(items):
    categories = {}
    for item in items:
        cat = item['category'].lower()
        if cat not in categories:
            categories[cat] = {'count': 0, 'quantity': 0, 'revenue': 0.0}
        categories[cat]['count'] += 1
        categories[cat]['quantity'] += item['quantity']
        categories[cat]['revenue'] += item['subtotal']

    for cat in categories:
        categories[cat]['revenue'] = round(categories[cat]['revenue'], 2)

    return categories


def detect_discount_pattern(items):
    discounted_items = [i for i in items if i['discount_percentage'] > 0]
    if not discounted_items:
        return {
            'uses_discounts': False,
            'num_discounted_items': 0,
            'avg_discount_percentage': 0,
            'total_savings': 0
        }

    avg_discount = sum(i['discount_percentage'] for i in discounted_items) / len(discounted_items)
    total_savings = sum(i['discount_amount'] for i in items)

    return {
        'uses_discounts': True,
        'num_discounted_items': len(discounted_items),
        'avg_discount_percentage': round(avg_discount, 2),
        'total_savings': round(total_savings, 2)
    }


def calculate_delivery_priority(order):
    score = 0
    if order['total_amount'] >= 500:
        score += 3
    if order['customer_city'] in ['Paris', 'Lyon', 'Marseille']:
        score += 2
    if order['status'] in ['shipped', 'confirmed']:
        score += 2

    if score >= 5:
        return 'urgent'
    elif score >= 3:
        return 'normal'
    else:
        return 'standard'


def validate_order(order):
    errors = []

    if order.get('total_amount', 0) <= 0:
        errors.append('Montant total invalide')
    if not order.get('order_id'):
        errors.append('ID commande manquant')
    if not order.get('items'):
        errors.append('Aucun article')

    calc_subtotal = sum(i['subtotal'] for i in order.get('items', []))
    if abs(calc_subtotal - order.get('subtotal', 0)) > 0.01:
        errors.append('Sous-total incohérent')

    return len(errors) == 0, errors


def process_order(order):
    is_valid, errors = validate_order(order)
    if not is_valid:
        print(f"⚠️ Commande invalide ({order.get('order_id')}): {', '.join(errors)}")
        return None

    order_date = datetime.fromisoformat(order['order_date'].replace('Z', '+00:00'))

    return {
        'order_id': order['order_id'],
        'order_date': order['order_date'],
        'customer_id': order['customer_id'],
        'customer_email': order['customer_email'],
        'customer_city': order['customer_city'],
        'status': order['status'],
        'payment_method': order['payment_method'],
        'items': order['items'],
        'num_items': order['num_items'],
        'total_quantity': order['total_quantity'],
        'subtotal': order['subtotal'],
        'total_discount': order['total_discount'],
        'shipping_cost': order['shipping_cost'],
        'tax_amount': order['tax_amount'],
        'total_amount': order['total_amount'],
        'order_value_class': classify_order_value(order['total_amount']),
        'customer_segment': classify_customer_segment(order['total_quantity'], order['total_amount']),
        'delivery_priority': calculate_delivery_priority(order),
        'profit_analysis': calculate_profit_margin(order['items']),
        'discount_pattern': detect_discount_pattern(order['items']),
        'product_mix': analyze_product_mix(order['items']),
        'average_item_value': round(order['subtotal'] / order['num_items'], 2),
        'discount_rate': round((order['total_discount'] / order['subtotal'] * 100), 2)
        if order['subtotal'] > 0 else 0,
        'tax_rate_applied': order.get('tax_rate', 0),
        'order_hour': order_date.hour,
        'order_day_of_week': order_date.strftime('%A'),
        'is_weekend_order': order_date.weekday() >= 5,
        'processed_at': datetime.utcnow().isoformat() + 'Z',
        'processor_version': '2.0'
    }


def lambda_handler(event, context):
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        print(f"📥 Traitement du fichier : s3://{bucket}/{key}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')

        # ✅ CORRECTION CLÉ : lecture JSONL ligne par ligne
        raw_orders = []
        for idx, line in enumerate(file_content.splitlines()):
            if line.strip():
                try:
                    raw_orders.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"❌ Ligne {idx + 1} invalide : {e}")

        processed_orders = []
        invalid_count = 0

        for order in raw_orders:
            processed = process_order(order)
            if processed:
                processed_orders.append(processed)
            else:
                invalid_count += 1

        print(f"✅ Commandes traitées : {len(processed_orders)} | Invalides : {invalid_count}")

        now = datetime.utcnow()
        s3_key = (
            f"{PREFIX_PROCESSED}"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
            f"processed_{now.strftime('%Y%m%d_%H%M%S')}.jsonl"
        )

        with io.StringIO() as buffer:
            for order in processed_orders:
                buffer.write(json.dumps(order))
                buffer.write('\n')

            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/json'
            )

        print(f"✅ Fichier traité et stocké : s3://{BUCKET_NAME}/{s3_key}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Traitement réussi',
                'total_processed': len(processed_orders),
                'invalid_orders': invalid_count,
                's3_location': f's3://{BUCKET_NAME}/{s3_key}'
            })
        }

    except Exception as e:
        print(f"❌ Erreur : {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
