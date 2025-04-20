import pandas as pd  
import sqlite3  

# Step 1: Load the data from CSV files  
shipping_data_0 = pd.read_csv(r'C:\Users\Austin Ibe\Desktop\Python Data Munging\forage-walmart-task-4\data\shipping_data_0.csv')  
shipping_data_1 = pd.read_csv(r'C:\Users\Austin Ibe\Desktop\Python Data Munging\forage-walmart-task-4\data\shipping_data_1.csv')  
shipping_data_2 = pd.read_csv(r'C:\Users\Austin Ibe\Desktop\Python Data Munging\forage-walmart-task-4\data\shipping_data_2.csv')  

# Print the columns of each DataFrame for debugging  
print("Columns in shipping_data_0:", shipping_data_0.columns.tolist())  
print("Columns in shipping_data_1:", shipping_data_1.columns.tolist())  
print("Columns in shipping_data_2:", shipping_data_2.columns.tolist())  

# Step 2: Connect to the SQLite database  
conn = sqlite3.connect(r'C:\Users\Austin Ibe\Desktop\Python Data Munging\forage-walmart-task-4\data\shipment_database.db')  
cursor = conn.cursor()  

# Step 3: Create the shipping_data_1_table if it doesn't exist  
cursor.execute('''  
CREATE TABLE IF NOT EXISTS shipping_data_1_table (  
    product_id TEXT,  
    quantity INTEGER,  
    origin TEXT,  
    destination TEXT  
)  
''')  

# Step 4: Insert data from shipping_data_0 into the database  
shipping_data_0.to_sql('shipping_data_0_table', conn, if_exists='append', index=False)  

# Step 5: Process shipping_data_1 and shipping_data_2  
if shipping_data_1.empty:  
    print("The DataFrame shipping_data_1 is empty.")  
else:  
    for index, row in shipping_data_1.iterrows():  
        shipment_identifier = row['shipment_identifier']  # Using the correct column name  
        product_id = row['product']  
        quantity = row['on_time']  # Ensure this is the intended quantity  
        
        # Find origin and destination from shipping_data_2  
        matching_row = shipping_data_2[shipping_data_2['shipment_identifier'] == shipment_identifier]  

        if not matching_row.empty:  
            origin = matching_row['origin_warehouse'].values[0]  
            destination = matching_row['destination_store'].values[0]  

            # Insert into the database for shipping_data_1  
            cursor.execute('''  
                INSERT INTO shipping_data_1_table (product_id, quantity, origin, destination)   
                VALUES (?, ?, ?, ?)  
            ''', (product_id, quantity, origin, destination))  

# Commit the transaction and close the connection  
conn.commit()  
conn.close()  

print("Database population complete!")  