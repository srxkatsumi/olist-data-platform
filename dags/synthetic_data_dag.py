from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
import random
import hashlib
from faker import Faker

fake = Faker('pt_BR')
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

BRAZILIAN_STATES = [
    'SP','RJ','MG','RS','PR','SC','BA','GO','PE','CE',
    'PA','MA','MS','MT','RN','AL','PB','SE','PI','TO',
    'RO','AM','AC','AP','RR','DF','ES'
]

ORDER_STATUSES = [
    'delivered','delivered','delivered','delivered','delivered',
    'shipped','canceled','invoiced','processing'
]

PAYMENT_TYPES = ['credit_card','credit_card','credit_card','boleto','voucher','debit_card']

CATEGORIES = [
    'health_beauty','watches_gifts','bed_bath_table','sports_leisure',
    'computers_accessories','furniture_decor','housewares','cool_stuff',
    'auto','toys','garden_tools','baby','perfumery','telephony'
]

def generate_fake_cpf():
    digits = [random.randint(0, 9) for _ in range(9)]
    for _ in range(2):
        val = sum((len(digits) + 1 - i) * v for i, v in enumerate(digits)) % 11
        digits.append(0 if val < 2 else 11 - val)
    return ''.join(map(str, digits))

def generate_credit_card_token(customer_id):
    return hashlib.sha256(f"{customer_id}{random.randint(1000,9999)}".encode()).hexdigest()[:32]

def create_extended_customers_table():
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_customers_extended (
                customer_id         VARCHAR(255) PRIMARY KEY,
                full_name           VARCHAR(255),
                email               VARCHAR(255),
                phone               VARCHAR(20),
                document_number     VARCHAR(11),
                credit_card_token   VARCHAR(64),
                credit_card_last4   VARCHAR(4),
                lgpd_accepted       BOOLEAN,
                lgpd_accepted_at    TIMESTAMP,
                marketing_opt_in    BOOLEAN,
                marketing_opt_in_at TIMESTAMP,
                customer_state      VARCHAR(2),
                customer_city       VARCHAR(255),
                created_at          TIMESTAMP,
                is_synthetic        BOOLEAN DEFAULT TRUE,
                data_source         VARCHAR(50) DEFAULT 'synthetic_2019'
            )
        """))
    print("Table dim_customers_extended created successfully!")

def generate_normal_customers(n=4500):
    engine = create_engine(DB_CONN)
    customers = []
    for i in range(n):
        state = random.choice(BRAZILIAN_STATES)
        lgpd_date = fake.date_time_between(start_date='-3y', end_date='now')
        marketing = random.random() > 0.3
        customers.append({
            'customer_id': f"synthetic_{i:06d}_{fake.uuid4()[:8]}",
            'full_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number()[:20],
            'document_number': generate_fake_cpf(),
            'credit_card_token': generate_credit_card_token(i),
            'credit_card_last4': str(random.randint(1000, 9999)),
            'lgpd_accepted': True,
            'lgpd_accepted_at': lgpd_date,
            'marketing_opt_in': marketing,
            'marketing_opt_in_at': lgpd_date if marketing else None,
            'customer_state': state,
            'customer_city': fake.city(),
            'created_at': fake.date_time_between(start_date='-3y', end_date='now'),
            'is_synthetic': True,
            'data_source': 'synthetic_2019'
        })
    df = pd.DataFrame(customers)
    df.to_sql('dim_customers_extended', engine, if_exists='append', index=False)
    print(f"{n} normal customers inserted!")

def generate_fraud_customers(n=500):
    engine = create_engine(DB_CONN)
    customers = []
    shared_card_token = generate_credit_card_token(99999)
    shared_card_last4 = str(random.randint(1000, 9999))
    shared_city = fake.city()
    shared_state = random.choice(BRAZILIAN_STATES)

    for i in range(n):
        lgpd_date = fake.date_time_between(start_date='-2y', end_date='now')
        customers.append({
            'customer_id': f"fraud_{i:06d}_{fake.uuid4()[:8]}",
            'full_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number()[:20],
            'document_number': generate_fake_cpf(),
            'credit_card_token': shared_card_token,
            'credit_card_last4': shared_card_last4,
            'lgpd_accepted': random.random() > 0.2,
            'lgpd_accepted_at': lgpd_date,
            'marketing_opt_in': random.random() > 0.5,
            'marketing_opt_in_at': lgpd_date,
            'customer_state': shared_state,
            'customer_city': shared_city,
            'created_at': fake.date_time_between(start_date='-2y', end_date='now'),
            'is_synthetic': True,
            'data_source': 'synthetic_fraud_2019'
        })
    df = pd.DataFrame(customers)
    df.to_sql('dim_customers_extended', engine, if_exists='append', index=False)
    print(f"{n} fraud customers inserted with shared card token!")

def generate_synthetic_orders():
    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        customers = pd.read_sql(
            "SELECT customer_id FROM dim_customers_extended WHERE is_synthetic = TRUE",
            conn
        )

    orders = []
    items = []
    payments = []
    reviews = []

    for _, row in customers.iterrows():
        n_orders = random.randint(1, 3)
        for _ in range(n_orders):
            order_id = fake.uuid4()
            purchase_date = fake.date_time_between(
                start_date=datetime(2019, 1, 1),
                end_date=datetime(2019, 12, 31)
            )
            status = random.choice(ORDER_STATUSES)
            delivered_date = None
            estimated_date = purchase_date + timedelta(days=random.randint(7, 30))

            if status == 'delivered':
                delivered_date = purchase_date + timedelta(days=random.randint(3, 45))

            orders.append({
                'order_id': order_id,
                'customer_id': row['customer_id'],
                'order_status': status,
                'order_purchase_timestamp': purchase_date,
                'order_approved_at': purchase_date + timedelta(hours=random.randint(1, 48)),
                'order_delivered_carrier_date': purchase_date + timedelta(days=random.randint(1, 5)),
                'order_delivered_customer_date': delivered_date,
                'order_estimated_delivery_date': estimated_date
            })

            n_items = random.randint(1, 3)
            total_value = 0
            for j in range(n_items):
                price = round(random.uniform(20, 500), 2)
                freight = round(random.uniform(5, 50), 2)
                total_value += price
                items.append({
                    'order_id': order_id,
                    'order_item_id': j + 1,
                    'product_id': fake.uuid4(),
                    'seller_id': fake.uuid4(),
                    'shipping_limit_date': purchase_date + timedelta(days=3),
                    'price': price,
                    'freight_value': freight
                })

            payment_type = random.choice(PAYMENT_TYPES)
            installments = random.randint(1, 12) if payment_type == 'credit_card' else 1
            payments.append({
                'order_id': order_id,
                'payment_sequential': 1,
                'payment_type': payment_type,
                'payment_installments': installments,
                'payment_value': round(total_value, 2)
            })

            if status == 'delivered':
                score = random.choices([1,2,3,4,5], weights=[5,5,10,30,50])[0]
                reviews.append({
                    'review_id': fake.uuid4(),
                    'order_id': order_id,
                    'review_score': score,
                    'review_comment_title': None,
                    'review_comment_message': None,
                    'review_creation_date': delivered_date + timedelta(days=random.randint(1, 7)),
                    'review_answer_timestamp': delivered_date + timedelta(days=random.randint(2, 10))
                })

    pd.DataFrame(orders).to_sql('olist_orders_dataset', engine, if_exists='append', index=False)
    pd.DataFrame(items).to_sql('olist_order_items_dataset', engine, if_exists='append', index=False)
    pd.DataFrame(payments).to_sql('olist_order_payments_dataset', engine, if_exists='append', index=False)
    pd.DataFrame(reviews).to_sql('olist_order_reviews_dataset', engine, if_exists='append', index=False)
    print(f"Synthetic orders generated: {len(orders)} orders, {len(items)} items, {len(payments)} payments, {len(reviews)} reviews!")

default_args = {
    "owner": "vicky",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="synthetic_data_dag",
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule=None,
    catchup=False,
    tags=["synthetic", "data-quality", "fraud"],
    doc_md="""
    ## Synthetic Data DAG

    Generates 5.000 synthetic customers and orders for 2019 testing purposes.
    
    **Important:** All data generated by this DAG is synthetic and marked with
    `is_synthetic = TRUE`. Data inserted after 2018 is for testing only.
    
    Includes intentional fraud patterns for anti-fraud model testing:
    - 500 customers sharing the same credit card token
    - Same city and state for fraud cluster
    - Orders from 2019 (post-dataset period)
    """,
) as dag:

    task_create_table = PythonOperator(
        task_id="create_extended_customers_table",
        python_callable=create_extended_customers_table,
    )

    task_normal_customers = PythonOperator(
        task_id="generate_normal_customers",
        python_callable=generate_normal_customers,
    )

    task_fraud_customers = PythonOperator(
        task_id="generate_fraud_customers",
        python_callable=generate_fraud_customers,
    )

    task_orders = PythonOperator(
        task_id="generate_synthetic_orders",
        python_callable=generate_synthetic_orders,
    )

    task_create_table >> task_normal_customers >> task_fraud_customers >> task_orders
