import pandas as pd


# Extract
customers = pd.read_csv("data/customers_raw.csv")
products = pd.read_csv("data/products_raw.csv")
orders = pd.read_csv("data/orders_raw.csv")

# Transform

# 1. Clean data
customers = customers.drop_duplicates().dropna()
products = products.drop_duplicates().dropna()
orders = orders.drop_duplicates().dropna()

# Standardize email format 
customers["email"] = customers["email"].str.lower()

# Standardize category lowercase
products["category"] = products["category"].str.lower()

# Standardize order_date
orders["order_date"] = pd.to_datetime(orders["order_date"])

# 2. Derive total_amount
orders = orders.merge(products[['product_id','price']], on = 'product_id')
orders['total_amount'] = orders['price']*orders['quantity']


# 3. Map to dimension tables
dim_customers = customers.rename(columns={
    "name": "customer_name"
})
dim_products = products.rename(columns={
    "name": "product_name"
})

dim_date = orders[['order_date']].drop_duplicates().copy()
dim_date['date_id'] = dim_date['order_date'].dt.strftime('%Y%m%d').astype(int)
dim_date['year'] = dim_date['order_date'].dt.year
dim_date['month'] = dim_date['order_date'].dt.month
dim_date['day'] = dim_date['order_date'].dt.day
dim_date['weekday'] = dim_date['order_date'].dt.day_name()

# Map date_id back to orders
orders = orders.merge(dim_date[["order_date", "date_id"]],
                      on="order_date")
# 4. Build fact_orders
fact_orders = orders[[
    "order_id", "customer_id", "product_id", "date_id", "quantity", "total_amount"
]]


# Load

dim_customers.to_csv("output/dim_customers.csv", index=False)
dim_products.to_csv("output/dim_products.csv", index=False)
dim_date.to_csv("output/dim_date.csv", index=False)
fact_orders.to_csv("output/fact_orders.csv", index=False)

print("ETL complete. Star schema CSVs created!")
