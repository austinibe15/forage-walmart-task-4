import pandas as pd  
import sqlite3  

# Step 1: Load the data from CSV files  
shipping_data_0 = pd.read_csv(r'c:\Users\Austin Ibe\Destop\Python Data Munging\forage-walmat-task-4\data\shipping_data_0.csv')  
shipping_data_1 = pd.read_csv(r'c:\Users\Austin Ibe\Destop\Python Data Munging\forage-walmat-task-4\data\shipping_data_1.csv')  
shipping_data_2 = pd.read_csv(r'c:\Users\Austin Ibe\Destop\Python Data Munging\forage-walmat-task-4\data\shipping_data_2.csv')  

# Step 2: Connect to the SQLite database  
conn = sqlite3.connect(r'c:\Users\Austin Ibe\Destop\Python Data Munging\forage-walmat-task-4\data\shipment_database.db')  # Correct database path  
cursor = conn.cursor()  

# Step 3: Insert data from shipping_data_0 into the database  
shipping_data_0.to_sql('shipping_data_0_table', conn, if_exists='append', index=False)  

# Step 4: Process shipping_data_1 and shipping_data_2  
# Assuming these are the columns in shipping_data_1 and shipping_data_2 CSV files  
for index, row in shipping_data_1.iterrows():  
    shipping_identifier = row['shipping_identifier']  
    product_id = row['product_id']  # Adjust this if the actual column name is different  
    quantity = row['quantity']  # Adjust this if the actual column name is different  

    # Find origin and destination from shipping_data_2  
    matching_row = shipping_data_2[shipping_data_2['shipping_identifier'] == shipping_identifier]  

    if not matching_row.empty:  
        origin = matching_row['origin'].values[0]  # Adjust this if the actual column name is different  
        destination = matching_row['destination'].values[0]  # Adjust this if the actual column name is different  

        # Insert into the database for shipping_data_1  
        cursor.execute('''  
            INSERT INTO shipping_data_1_table (product_id, quantity, origin, destination)   
            VALUES (?, ?, ?, ?)  
        ''', (product_id, quantity, origin, destination))  

# Commit the transaction and close the connection  
conn.commit()  
conn.close()  

print("Database population complete!")  