import psycopg2
import pandas as pd
import time


# INSERT DATA
def insert_data(cursor, user_data, transaction_data):
    for user in user_data:
        cursor.execute(
            "INSERT INTO users (id, name, age, phone, city, gender, ward, lat, lng, current_bal) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                user["id"],
                user["name"],
                user["age"],
                user["phone"],
                user["city"],
                user["gender"],
                user["ward"],
                user["lat"],
                user["lng"],
                user["current_bal"],
            ),
        )

    for transaction in transaction_data:
        cursor.execute(
            "INSERT INTO transactions (id, user_id, amount, time, status, type, balance)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                transaction["id"],
                transaction["user_id"],
                transaction["amount"],
                transaction["time"],
                transaction["status"],
                transaction["type"],
                transaction["balance"],
            ),
        )


# FUND USER QUERY
def finduser(cursor):
    try:
        cursor.execute(
            "SELECT * FROM users WHERE city='dhaka' AND ward='babu_bazar' AND age='17' AND (gender='male' OR gender='female')"
        )
        data = cursor.fetchall()
        return data
    except Exception as error:
        print("An error occured", error)


def resetDb(cursor):
    cursor.execute("TRUNCATE TABLE users")
    cursor.execute("TRUNCATE TABLE transactions")


def main():
    # DATABASE CONFIG
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="1591",
        host="localhost",
        port="5432",
    )

    cursor = conn.cursor()
    resetDb(cursor)

    user_xlsx_path = "test02.xls"
    transaction_xlsx_path = "test03.xls"

    user_df = pd.read_excel(user_xlsx_path)
    transaction_df = pd.read_excel(transaction_xlsx_path)
    user_data = user_df.to_dict(orient="records")
    transaction_data = transaction_df.to_dict(orient="records")

    # START TIME
    start_time = time.time()

    # INSERT FUNCTION CALL
    insert_data(cursor, user_data, transaction_data)

    # QUERY
    res = finduser(cursor)
    print("-----------", res)

    # END TIME
    end_time = time.time()

    # CAL EXE TIME
    execution_time = end_time - start_time
    print("Total Execution Time: {:.6f} seconds".format(execution_time))

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
