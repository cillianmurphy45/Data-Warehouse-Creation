import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
load_dotenv()

car_sales = pd.read_csv("Data/Cleaned_Car_Sales_Data.csv")

conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_data (
        Car_ID VARCHAR(100), Sale_Date DATE, Customer_Name VARCHAR(100), Gender VARCHAR(20), Annual_Income INT,
       Dealer_Name VARCHAR(100), Car_Manufacturer VARCHAR(100), Car_Model VARCHAR(100), Engine_Type VARCHAR(100),
       Transmission VARCHAR(100), Colour VARCHAR(100), Sale_Price VARCHAR(100), Dealer_ID VARCHAR(10), Body_Style VARCHAR(100),
       Customer_Phone_Number VARCHAR(100), Dealer_Region VARCHAR(100), Dealer_State VARCHAR(100))""")

#for i, car_sale_row in car_sales.iterrows():
 #   car_sql = """INSERT INTO raw_data (Car_ID , Sale_Date , Customer_Name , Gender , Annual_Income ,
  #     Dealer_Name , Car_Manufacturer , Car_Model , Engine_Type ,
   #    Transmission , Colour , Sale_Price , Dealer_ID , Body_Style ,
    #   Customer_Phone_Number , Dealer_Region , Dealer_State ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

 #   car_values = tuple(car_sale_row)

#    cur.execute(car_sql, car_values)

#conn.commit()


cur.execute("""
CREATE TABLE IF NOT EXISTS Dim_Customer (
            Customer_ID INT AUTO_INCREMENT PRIMARY KEY,
            Customer_Name VARCHAR(255),
            Gender VARCHAR(20),
            Customer_Phone_Number VARCHAR(50)
            );""")

cust_sql = """
    INSERT INTO Dim_Customer (Customer_Name, Gender, Customer_Phone_Number)
    SELECT DISTINCT Customer_Name, Gender, Customer_Phone_Number
    FROM raw_data"""

#cur.execute(cust_sql)
#conn.commit()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Car (
                Car_ID INT AUTO_INCREMENT PRIMARY KEY,
                Car_Manufacturer VARCHAR(50),
                Car_Model VARCHAR(50),
                Engine_Type VARCHAR(50),
                Transmission VARCHAR(50),
                Colour VARCHAR(50),
                Body_Style VARCHAR(50)
            );
    """)

car_sql = """
    INSERT INTO Dim_Car (Car_Manufacturer, Car_Model, Engine_Type, Transmission, Colour, Body_Style)
    SELECT DISTINCT Car_Manufacturer, Car_Model, Engine_Type, Transmission, Colour, Body_Style
    FROM raw_data"""

#cur.execute(car_sql)
#conn.commit()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Date (
                Date_ID INT AUTO_INCREMENT PRIMARY KEY,
                Sale_Date DATE,
                Year VARCHAR(50),
                Quarter INT,
                Month VARCHAR(50),
                Day VARCHAR(50)
            );
    """)
date_sql = """
    INSERT INTO Dim_Date (Sale_Date, Year, Quarter, Month, Day)
    SELECT DISTINCT Sale_Date, YEAR(Sale_Date) AS Year , QUARTER(Sale_Date) AS Quarter, MONTHNAME(Sale_Date) AS Month, DAYNAME(Sale_Date) AS Day 
    FROM raw_data"""

#cur.execute(date_sql)
#conn.commit()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Region (
                Region_ID INT AUTO_INCREMENT PRIMARY KEY,
                Dealer_Region VARCHAR(50),
                Dealer_State VARCHAR(50)
            );
    """)

region_sql = """
    INSERT INTO Dim_Region (Dealer_Region, Dealer_State)
    SELECT DISTINCT Dealer_Region, Dealer_State
    FROM raw_data"""

#cur.execute(region_sql)
#conn.commit()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Dealer (
            Dealer_ID VARCHAR(10) PRIMARY KEY,
            Dealer_Name VARCHAR(200),
            Region_ID INT,
            FOREIGN KEY (Region_ID) REFERENCES Dim_Region(Region_ID)

            );
    """)

region_sql = """
    INSERT INTO Dim_Dealer (Dealer_ID, Dealer_Name, Region_ID)
    SELECT DISTINCT raw_data.Dealer_ID, MIN(raw_data.Dealer_Name), 
    dim_region.Region_ID FROM raw_data JOIN dim_region ON raw_data.Dealer_Region = dim_region.Dealer_Region 
    GROUP BY raw_data.Dealer_ID
    """

#cur.execute(region_sql)
#conn.commit()

cur.execute("""
    CREATE TABLE IF NOT EXISTS Fact_Sales (
        Sale_ID INT AUTO_INCREMENT PRIMARY KEY,
        Car_ID INT,
        Customer_ID INT,
        Date_ID INT,
        Dealer_ID VARCHAR(10),
        Sale_Price INT,
        Annual_Income INT,
        FOREIGN KEY (Car_ID) REFERENCES Dim_Car(Car_ID),
        FOREIGN KEY (Customer_ID) REFERENCES Dim_Customer(Customer_ID),
        FOREIGN KEY (Date_ID) REFERENCES Dim_Date(Date_ID),
        FOREIGN KEY (Dealer_ID) REFERENCES Dim_Dealer(Dealer_ID)

        );""")

fact_sql = """
    INSERT INTO Fact_Sales (Car_ID, Customer_ID, Date_ID, Dealer_ID, Sale_Price, Annual_Income)
    SELECT
        dcar.Car_ID,
        dcust.Customer_ID,
        ddate.Date_ID,
        ddeal.Dealer_ID,
        raw.Sale_Price,
        raw.Annual_Income
    FROM raw_data raw, dim_car dcar, dim_customer dcust, dim_date ddate, dim_dealer ddeal
    WHERE dcar.Car_Manufacturer = raw.Car_Manufacturer 
        AND dcar.Car_Model = raw.Car_Model 
        AND dcar.Engine_Type = raw.Engine_Type 
        AND dcar.Transmission = raw.Transmission 
        AND dcar.Colour = raw.Colour 
        AND dcar.Body_Style = raw.Body_Style
        AND dcust.Customer_Name = raw.Customer_Name 
        AND dcust.Gender = raw.Gender 
        AND dcust.Customer_Phone_Number = raw.Customer_Phone_Number
        AND ddate.Sale_Date = raw.Sale_Date
        AND ddeal.Dealer_Name = raw.Dealer_Name
    """

cur.execute(fact_sql)
conn.commit()