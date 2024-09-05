import tkinter as tk
from tkinter import filedialog
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
import csv

def connect_to_database():
    # σύνδεση με βάση δεδομένων
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='memosgianna2002',
        port='3306',
        database='python'
    )
    return mydb

def open_file():
    filepath = filedialog.askopenfilename(filetypes=[('CSV Files', '*.csv')])
    if filepath:
        with open(filepath, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in reader:
                print(row)
        plot_data(filepath)
        plot_total_per_country(filepath)

# δημιουργία main menu
window = tk.Tk()
window.title("Data Analysis")



def plot_data():
        # σύνδεση με τη βάση δεδομένων
        db_connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='memosgianna2002',
            database='python'
        )
        cursor = db_connection.cursor()

        # δημιουργία του πίνακα erwthma1
        create_table_query = """
            CREATE TABLE IF NOT EXISTS erwthma1 (
                Date DATE,
                Measure VARCHAR(255),
                Value FLOAT
            )
        """
        cursor.execute(create_table_query)

        # διάβασμα του αρχείου CSV
        df = pd.read_csv('python.csv')
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
        df_monthly_sales = df.groupby([pd.Grouper(key='Date', freq='M'), 'Measure'])['Value'].sum().reset_index()

        # εισαγωγή των δεδομένων στον πίνακα erwthma1
        insert_query = "INSERT INTO erwthma1 (Date, Measure, Value) VALUES (%s, %s, %s)"
        for _, row in df_monthly_sales.iterrows():
            insert_data = (row['Date'].date(), row['Measure'], row['Value'])
            cursor.execute(insert_query, insert_data)

        # αποθήκευση των αλλαγών στη βάση δεδομένων
        db_connection.commit()

        # κλείσιμο της σύνδεσης με τη βάση δεδομένων
        cursor.close()
        db_connection.close()

        # εμφάνιση γραφήματος
        fig, ax = plt.subplots(figsize=(10, 6))
        for label, grp in df_monthly_sales.groupby('Measure'):
            ax = grp.plot(ax=ax, kind='bar', x='Date', y='Value', label=label)
            ax.set_xlabel('Month')
            ax.set_ylabel('Tziros')
        plt.show()
        # μήνυμα επιβεβαίωσης
        print("Data saved to SQL table successfully.")

        # εξαγωγή αποτελεσμάτων σε .csv

        output_file = 'erwthma1.csv'
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Date', 'Measure', 'Value'])
            for _, row in df_monthly_sales.iterrows():
                writer.writerow([row['Date'], row['Measure'], row['Value']])

        print(f"Τα αποτελέσματα εξήχθησαν με επιτυχία στο αρχείο {output_file}.")


def plot_total_per_country():
    country_total = {}

    # διάβασμα αρχείου

    with open('python.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['Country'] not in country_total:
                country_total[row['Country']] = 0
            country_total[row['Country']] += int(row['Value'])

    # σύνδεση με τη βάση δεδομένων
    db_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='memosgianna2002',
        database='python'
    )

    # δημιουργία του πίνακα revenue_data
    create_table_query = """
    CREATE TABLE IF NOT EXISTS revenue_data (
        Country VARCHAR(255),
        TotalValue BIGINT
    )
    """
    cursor = db_connection.cursor()
    cursor.execute(create_table_query)

    # εισαγωγή των δεδομένων στον πίνακα
    insert_query = "INSERT INTO revenue_data (Country, TotalValue) VALUES (%s, %s)"
    for country, total_value in country_total.items():
        data = (country, total_value)
        cursor.execute(insert_query, data)

    # αποθήκευση των αλλαγών στη βάση δεδομένων
    db_connection.commit()

    # κλείσιμο της σύνδεσης με τη βάση δεδομένων
    db_connection.close()

    # δημιουργία γραφήματος
    plt.bar(country_total.keys(), country_total.values())
    plt.xticks(rotation=90)
    plt.ylabel('Total Value')
    plt.xlabel('Country')
    plt.title('Total Value per Country')


    plt.show()

    print("Data saved to SQL table successfully.")

    # εξαγωγή των αποτελεσμάτων σε ένα αρχείο CSV
    output_file = 'erwthma2.csv'
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Country', 'TotalValue'])
        for country, total_value in country_total.items():
            writer.writerow([country, total_value])
    print(f"Τα αποτελέσματα εξάχθηκαν με επιτυχία στο αρχείο {output_file}.")


def plot_total_by_transport_mode():

    # δημιουργία dictionary για το άθροισμα τζίρου για κάθε transport_mode
    mode_total = {}

    with open('python.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['Transport_Mode'] not in mode_total:
                mode_total[row['Transport_Mode']] = 0
            mode_total[row['Transport_Mode']] += int(row['Value'])

        # σύνδεση με τη βάση δεδομένων
        db_connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='memosgianna2002',
            database='python'
        )

        # δημιουργία του πίνακα commodity_table για την αποθήκευση των δεδομένων
        create_table_query = """
        CREATE TABLE IF NOT EXISTS commodity_table (
            Comodity VARCHAR(255),
            TotalValue BIGINT
        )
        """
        cursor = db_connection.cursor()
        cursor.execute(create_table_query)

        # εισαγωγή των δεδομένων στον πίνακα
        insert_query = "INSERT INTO commodity_table (Comodity, TotalValue) VALUES (%s, %s)"
        for Comodity, total_value in mode_total.items():
            data = (Comodity, total_value)
            cursor.execute(insert_query, data)

    # αποθήκευση των αλλαγών στη βάση δεδομένων
    db_connection.commit()

    # κλείσιμο της σύνδεσης με τη βάση δεδομένων
    db_connection.close()

    # δημιουργία γραφήματος
    plt.bar(mode_total.keys(), mode_total.values())
    plt.xticks(rotation=90)
    plt.ylabel('Total Value')
    plt.xlabel('Transport Mode')
    plt.title('Total Value by Transport Mode')
    plt.show()
    print("Data saved to SQL table successfully.")

    # εξαγωγή των αποτελεσμάτων σε ένα αρχείο CSV
    output_file = 'erwthma3.csv'
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Transport_Mode', 'TotalValue'])
        for mode, total_value in mode_total.items():
            writer.writerow([mode, total_value])
    print(f"Τα αποτελέσματα εξάχθηκαν με επιτυχία στο αρχείο {output_file}.")


# erwthma 4
def plot_total_by_weekday():


    # δημιουργία λιστών για κάθε μέρα
    monday_total = []
    tuesday_total = []
    wednesday_total = []
    thursday_total = []
    friday_total = []
    saturday_total = []
    sunday_total = []

    with open('python.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['Weekday'] == 'Monday':
                monday_total.append(float(row['Value']))
            elif row['Weekday'] == 'Tuesday':
                tuesday_total.append(float(row['Value']))
            elif row['Weekday'] == 'Wednesday':
                wednesday_total.append(float(row['Value']))
            elif row['Weekday'] == 'Thursday':
                thursday_total.append(float(row['Value']))
            elif row['Weekday'] == 'Friday':
                friday_total.append(float(row['Value']))
            elif row['Weekday'] == 'Saturday':
                saturday_total.append(float(row['Value']))
            elif row['Weekday'] == 'Sunday':
                sunday_total.append(float(row['Value']))

    #  υπολογισμός συνολικού τζίρου για κάθε μέρα της εβδομάδας
    totals = [sum(monday_total), sum(tuesday_total), sum(wednesday_total), sum(thursday_total), sum(friday_total), sum(saturday_total), sum(sunday_total)]

    # λίστα για κάθε μία μερα της εβδομάδας
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # σύνδεση με τη βάση δεδομένων
    db_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='memosgianna2002',
        database='python'
    )

    # δημιουργία του πίνακα ebdomada για την αποθήκευση των δεδομένων
    create_table_query = """
           CREATE TABLE IF NOT EXISTS ebdomada (
               Weekdays VARCHAR(255),
               TotalValue BIGINT
           )
           """
    cursor = db_connection.cursor()
    cursor.execute(create_table_query)

    # εισαγωγή των δεδομένων στον πίνακα
    insert_query = "INSERT INTO ebdomada (Weekdays, TotalValue) VALUES (%s, %s)"
    for day, total_value in zip(days, totals):
        data = (day, total_value)
        cursor.execute(insert_query, data)

    # αποθήκευση των αλλαγών στη βάση δεδομένων
    db_connection.commit()

    # κλείσιμο της σύνδεσης με τη βάση δεδομένων
    db_connection.close()

    # δημιουργία γραφήματος
    plt.bar(days, totals)
    plt.title('Total Revenue by Day of Week')
    plt.xlabel('Day of Week')
    plt.ylabel('Total Revenue')
    plt.show()
    print("Data saved to SQL table successfully.")

    # εξαγωγή των αποτελεσμάτων σε ένα αρχείο CSV
    output_file = 'erwthma4.csv'
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Weekday', 'TotalValue'])
        for day, total_value in zip(days, totals):
            writer.writerow([day, total_value])
    print(f"Τα αποτελέσματα εξάχθηκαν με επιτυχία στο αρχείο {output_file}.")


#erwthma5
def erwthma5():
    # δημιουργία ενός λεξικού για τον υπολογισμό του συνολικού τζίρου για κάθε commodity
    commodity_total = {}

    with open('python.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            commodity = row['Commodity']
            if commodity not in commodity_total:
                commodity_total[commodity] = 0
            commodity_total[commodity] += int(row['Value'])

    # σύνδεση με τη βάση δεδομένων
    db_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='memosgianna2002',
        database='python'
    )
    cursor = db_connection.cursor()

    # δημιουργία του πίνακα erwthma5 αν δεν υπάρχει
    create_table_query = """
        CREATE TABLE IF NOT EXISTS erwthma5 (
            Commodity VARCHAR(255),
            TotalValue BIGINT
        )
    """
    cursor.execute(create_table_query)

    # εισαγωγή των δεδομένων στον πίνακα erwthma5
    insert_query = "INSERT INTO erwthma5 (Commodity, TotalValue) VALUES (%s, %s)"
    for commodity, total_value in commodity_total.items():
        data = (commodity, total_value)
        cursor.execute(insert_query, data)

    # αποθήκευση των αλλαγών στη βάση δεδομένων
    db_connection.commit()

    # κλείσιμο της σύνδεσης με τη βάση δεδομένων
    db_connection.close()

    # δημιουργία γραφήματος
    plt.bar(commodity_total.keys(), commodity_total.values())
    plt.xticks(rotation=90)
    plt.ylabel('Total Value')
    plt.xlabel('Commodity')
    plt.title('Total Value by Commodity')
    plt.show()
    print("Data saved to SQL table successfully.")

    # εξαγωγή των αποτελεσμάτων σε ένα αρχείο CSV
    output_file = 'erwthma5.csv'
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Commodity', 'TotalValue'])
        for commodity, total_value in commodity_total.items():
            writer.writerow([commodity, total_value])
    print(f"Τα αποτελέσματα εξάχθηκαν με επιτυχία στο αρχείο {output_file}.")


#Erwthma6
def erwthma6():
    # διάβασμα του αρχείο CSV στο DataFrame της Pandas
    df = pd.read_csv('python.csv', parse_dates=['Date'], dayfirst=True)

    # group by τα δεδομένα ανά μήνα και υπολογισμός συνολικού τζίρου για κάθε μήνα
    monthly_totals = df.groupby(pd.Grouper(key='Date', freq='M'))['Value'].sum().reset_index()

    # ταξινόμιση των δεδομένων κατά φθίνουσα σειρά τζίρου
    sorted_totals = monthly_totals.sort_values(by='Value', ascending=False)

    # επιλογή των 5 μηνών με το μεγαλύτερο τζίρο
    top_five_months = sorted_totals.head(5)

    # σύνδεση με τη βάση δεδομένων
    db_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='memosgianna2002',
        database='python'
    )
    cursor = db_connection.cursor()

    # δημιουργία του πίνακα erwthma6
    create_table_query = """
        CREATE TABLE IF NOT EXISTS erwthma6 (
            MonthYear VARCHAR(255),
            TotalValue BIGINT
        )
    """
    cursor.execute(create_table_query)

    # εισαγωγή των δεδομένων στον πίνακα erwthma6
    insert_query = "INSERT INTO erwthma6 (MonthYear, TotalValue) VALUES (%s, %s)"
    for index, row in top_five_months.iterrows():
        data = (row['Date'].strftime('%B %Y'), row['Value'])
        cursor.execute(insert_query, data)

    # αποθήκευση των αλλαγών στη βάση δεδομένων
    db_connection.commit()

    # κλείσιμο της σύνδεσης με τη βάση δεδομένων
    db_connection.close()


    # δημιουργία γραφήματος
    plt.bar(top_five_months['Date'].dt.strftime('%B %Y'), top_five_months['Value'])
    plt.xlabel('Month')
    plt.ylabel('Total Value')
    plt.title('Top 5 Months by Total Value')
    plt.xticks(rotation=45)
    plt.show()
    print("Data saved to SQL table successfully.")

    # εξαγωγή των αποτελεσμάτων σε ένα αρχείο CSV
    output_file = 'erwthma6.csv'
    top_five_months.to_csv(output_file, index=False)
    print(f"Τα αποτελέσματα εξάχθηκαν με επιτυχία στο αρχείο {output_file}.")


#erwthma7
def erwthma7():
    cnx = mysql.connector.connect(
        host='localhost',
        user='root',
        password='memosgianna2002',
        database='python'
    )
    cursor = cnx.cursor()

    # δημιουργία του πίνακα erwthma6
    create_table_query = """
    CREATE TABLE IF NOT EXISTS erwthma7 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country VARCHAR(255),
        category VARCHAR(255),
        value FLOAT
    )
    """
    cursor.execute(create_table_query)

    # διάβασμα του αρχείο CSV και αποθήκευση δεδομένων σε ένα λεξικό
    data = {}
    with open('python.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            country = row['Country']
            category = row['Commodity']
            value = float(row['Value'])
            if country not in data:
                data[country] = {}
            if category not in data[country]:
                data[country][category] = 0
            data[country][category] += value

    # δημιουργία γραφήματος
    plt.figure()

    for country in data:
        sorted_categories = sorted(data[country], key=data[country].get, reverse=True)
        top_categories = sorted_categories[:5]

        plt.bar(top_categories, [data[country][category] for category in top_categories], label=country)

    plt.xlabel('Category')
    plt.ylabel('Total Value')
    plt.title('Top 5 Categories by Country')
    plt.xticks(rotation=45)
    plt.tight_layout()  # προσθήκη για βελτιστοποίηση της διάταξης των γραφημάτων
    plt.legend()
    plt.show()

    # εισαγωγή των δεδομένων στον πίνακα erwthma7
    insert_query = "INSERT INTO erwthma7 (country, category, value) VALUES (%s, %s, %s)"
    for country in data:
        sorted_categories = sorted(data[country], key=data[country].get, reverse=True)
        top_categories = sorted_categories[:5]

        for category in top_categories:
            value = data[country][category]
            insert_data = (country, category, value)
            cursor.execute(insert_query, insert_data)

    # αποθήκευση των αλλαγών στη βάση δεδομένων
    cnx.commit()

    # κλείσιμο της σύνδεσης με τη βάση δεδομένων
    cursor.close()
    cnx.close()

    print("Data saved to SQL table successfully.")
    # εξαγωγή των αποτελεσμάτων σε ένα αρχείο CSV
    output_filename = 'erwthma7.csv'

    with open(output_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Country', 'Category', 'Value'])

        for country in data:
            sorted_categories = sorted(data[country], key=data[country].get, reverse=True)
            top_categories = sorted_categories[:5]

            for category in top_categories:
                value = data[country][category]
                writer.writerow([country, category, value])

        print("Τα αποτελέσματα εξάχθηκαν με επιτυχία στο αρχείο {output_file}.")


#erwthma8

def erwthma8():
    cnx = mysql.connector.connect(
        host='localhost',
        user='root',
        password='memosgianna2002',
        database='python'
    )
    cursor = cnx.cursor()

    # δημιουργία του πίνακα erwthma6
    create_table_query = """
    CREATE TABLE if not exists erwthma8 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        commodity VARCHAR(255),
        date DATE,
        value FLOAT
    )
    """
    cursor.execute(create_table_query)

    # φορτώνουμε τα δεδομένα από το αρχείο CSV
    df = pd.read_csv('python.csv', parse_dates=['Date'], dayfirst=True)

    # υπολογισμός συνολικών εσόδων για κάθε commodity για κάθε μέρα
    df_revenue = df.groupby(['Commodity', 'Date'])['Value'].sum().reset_index()

    # βρίσκω τις ημερομηνίες με τα μεγαλύτερα έσοδα για κάθε commodity
    df_max_revenue = df_revenue.loc[df_revenue.groupby('Commodity')['Value'].idxmax()]

    # μετατροπή της στήλη Date σε αντικείμενα datetime
    df_max_revenue['Date'] = pd.to_datetime(df_max_revenue['Date'], format='%d/%m/%Y')

    # εισαγωγή των αποτελεσμάτων στη βάση δεδομένων
    for _, row in df_max_revenue.iterrows():
        commodity = row['Commodity']
        date = row['Date']
        value = row['Value']

        #εισαγωγή των δεδομένων στον πίνακα erwthma8
        insert_query = "INSERT INTO erwthma8 (commodity, date, value) VALUES (%s, %s, %s)"
        insert_data = (commodity, date, value)
        cursor.execute(insert_query, insert_data)

    # αποθήκευση των αλλαγών στη βάση δεδομένων
    cnx.commit()

    # κλείσιμο της σύνδεσης με τη βάση δεδομένων
    cursor.close()
    cnx.close()

    # εμφάνιση του γραφήματος
    fig, ax = plt.subplots()

    # δημιουργία γραφήματος
    for _, row in df_max_revenue.iterrows():
        commodity = row['Commodity']
        date = row['Date']
        value = row['Value']

        ax.bar(commodity, value)

    ax.set_xlabel('Commodity')
    ax.set_ylabel('Total Value')
    ax.set_title('Maximum Revenue Date for Each Commodity')
    ax.tick_params(axis='x', rotation=90)

    plt.tight_layout()
    plt.show()

    print("Data saved to SQL table successfully.")

    # εξαγωγή των αποτελεσμάτων σε ένα αρχείο CSV
    output_file = 'erwthma8.csv'
    df_max_revenue.to_csv(output_file, index=False)
    print(f"Τα αποτελέσματα εξάχθηκαν με επιτυχία στο αρχείο {output_file}.")


# δημιουργία συνάρτησης για κάθε μια επιλογή του μενού
menu_options = [
    ("Erwthma 1", plot_data),
    ("Erwthma 2", plot_total_per_country),
    ("Erwthma 3", plot_total_by_transport_mode),
    ("Erwthma 4", plot_total_by_weekday),
    ("Erwthma 5", erwthma5),
    ("Erwthma 6", erwthma6),
    ("Erwthma 7", erwthma7),
    ("Erwthma 8", erwthma8)
]

# δυναμική δημιουργία των κουμπιών
for i, (option_text, option_command) in enumerate(menu_options):
    button = tk.Button(window, text=option_text, command=option_command)
    button.grid(row=i, column=0, padx=10, pady=5)

# εκτέλεση του gui menu
window.mainloop()
