# -*- coding: utf-8 -*-
"""
Created on Wed Oct  4 14:41:30 2023

@author: Mosco
"""

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Boolean, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class DimIndividualCustomer(Base):
    __tablename__ = 'dim_individual_customer'
    individual_customer_key = Column(Integer, primary_key=True)
    person_id = Column(String)
    gender = Column(String)
    marital_status = Column(String)
    education = Column(String)
    occupation = Column(String)
    home_owner_flag = Column(Boolean)
    city = Column(String)
    state_province = Column(String)
    country_region = Column(String)
    sales_orders = relationship('FactSalesOrder', back_populates='individual_customer')

class DimEmployee(Base):
    __tablename__ = 'dim_employee'
    employee_key = Column(Integer, primary_key=True)
    employee_id = Column(String)
    gender = Column(String)
    marital_status = Column(String)
    hire_date = Column(Date)
    job_title = Column(String)
    employee_name = Column(String)
    sales_orders = relationship('FactSalesOrder', back_populates='employee')

class DimStore(Base):
    __tablename__ = 'dim_store'
    store_key = Column(Integer, primary_key=True)
    store_id = Column(String)
    store_name = Column(String)
    year_opened = Column(Integer)
    square_feet = Column(Integer)
    specialty = Column(String)
    business_type = Column(String)
    city = Column(String)
    state_province = Column(String)
    country_region = Column(String)
    sales_orders = relationship('FactSalesOrder', back_populates='store')

class DimDate(Base):
    __tablename__ = 'dim_date'
    date_key = Column(Integer, primary_key=True)
    sql_date = Column(Date)
    day_of_week = Column(Integer)
    day_name_of_week = Column(String)
    day_of_month = Column(Integer)
    week_of_year = Column(Integer)
    month_of_year = Column(Integer)
    month_name = Column(String)
    quarter = Column(Integer)
    year = Column(Integer)

class DimTerritory(Base):
    __tablename__ = 'dim_territory'
    territory_key = Column(Integer, primary_key=True)
    territory_id = Column(String)
    name = Column(String)
    country_region_name = Column(String)
    group_name = Column(String)
    sales_orders = relationship('FactSalesOrder', back_populates='territory')

class FactSalesOrder(Base):
    __tablename__ = 'fact_sales_order'
    data_key = Column(Integer, primary_key=True)
    territory_key = Column(Integer, ForeignKey('dim_territory.territory_key'))
    territory = relationship('DimTerritory', back_populates='sales_orders')
    employee_key = Column(Integer, ForeignKey('dim_employee.employee_key'))
    employee = relationship('DimEmployee', back_populates='sales_orders')
    store_key = Column(Integer, ForeignKey('dim_store.store_key'))
    store = relationship('DimStore', back_populates='sales_orders')
    individual_customer_key = Column(Integer, ForeignKey('dim_individual_customer.individual_customer_key'))
    individual_customer = relationship('DimIndividualCustomer', back_populates='sales_orders')
    sales_order_number = Column(String)
    online_order_flag = Column(Boolean)
    sale_subtotal = Column(Float)
    tax_amount = Column(Float)
    shipping_cost = Column(Float)

# Setting up the database
engine = create_engine('sqlite:///sales_database.db')  # Using SQLite for demonstration
Base.metadata.create_all(engine)





from sqlalchemy.orm import sessionmaker
import datetime

# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Adding records to dim_individual_customer
customer1 = DimIndividualCustomer(
    person_id='P001', gender='Male', marital_status='Single',
    education='Graduate', occupation='Engineer',
    home_owner_flag=True, city='New York', state_province='NY', country_region='USA'
)
customer2 = DimIndividualCustomer(
    person_id='P002', gender='Female', marital_status='Married',
    education='Masters', occupation='Doctor',
    home_owner_flag=False, city='Los Angeles', state_province='CA', country_region='USA'
)
session.add_all([customer1, customer2])

# Adding records to dim_employee
employee1 = DimEmployee(
    employee_id='E001', gender='Female', marital_status='Single',
    hire_date=datetime.date(2023, 5, 15), job_title='Sales Manager', employee_name='Alice'
)
employee2 = DimEmployee(
    employee_id='E002', gender='Male', marital_status='Married',
    hire_date=datetime.date(2019, 10, 1), job_title='Sales Executive', employee_name='Bob'
)
session.add_all([employee1, employee2])

# Adding records to dim_store
store1 = DimStore(
    store_id='S001', store_name='Store A', year_opened=2018,
    square_feet=5000, specialty='Electronics', business_type='Retail',
    city='San Francisco', state_province='CA', country_region='USA'
)
store2 = DimStore(
    store_id='S002', store_name='Store B', year_opened=2019,
    square_feet=3000, specialty='Clothing', business_type='Retail',
    city='Seattle', state_province='WA', country_region='USA'
)
session.add_all([store1, store2])

# Adjusting the record for dim_date
date1 = DimDate(
    sql_date=datetime.date(2023, 9, 1),  # Use datetime.date here
    day_of_week=6, day_name_of_week='Saturday',
    day_of_month=1, week_of_year=35, month_of_year=9,
    month_name='September', quarter=3, year=2023
)
session.add(date1)

# Adding records to dim_territory
territory1 = DimTerritory(
    territory_id='T001', name='West Coast', country_region_name='USA', group_name='North America'
)
territory2 = DimTerritory(
    territory_id='T002', name='East Coast', country_region_name='USA', group_name='North America'
)
session.add_all([territory1, territory2])

# Adding records to fact_sales_order
sales_order1 = FactSalesOrder(
    territory=territory1, employee=employee1, store=store1, individual_customer=customer1,
    sales_order_number='SO001', online_order_flag=True, sale_subtotal=1000.00, tax_amount=100.00, shipping_cost=20.00
)
sales_order2 = FactSalesOrder(
    territory=territory2, employee=employee2, store=store2, individual_customer=customer2,
    sales_order_number='SO002', online_order_flag=False, sale_subtotal=500.00, tax_amount=50.00, shipping_cost=10.00
)
session.add_all([sales_order1, sales_order2])

# Committing the records to the database
session.commit()

# Close the session
session.close()



# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Fetch all customers:

customers = session.query(DimIndividualCustomer).all()
for customer in customers:
    print(customer.individual_customer_key, customer.person_id, customer.occupation, customer.city)


# Fetch all sales orders and their associated details:
    
sales_orders = session.query(FactSalesOrder).all()
for order in sales_orders:
    print(order.sales_order_number, order.sale_subtotal, order.tax_amount, 
          order.employee.employee_name, order.individual_customer.person_id)


# Fetch all stores and their locations:

stores = session.query(DimStore).all()
for store in stores:
    print(store.store_name, store.city, store.state_province, store.country_region)

# Find sales orders by a specific employee:

employee_name_to_search = "Alice"
orders_by_employee = session.query(FactSalesOrder).join(DimEmployee).filter(DimEmployee.employee_name == employee_name_to_search).all()
for order in orders_by_employee:
    print(order.sales_order_number, order.sale_subtotal)

# Find sales from a specific territory:
territory_name_to_search = "West Coast"
orders_from_territory = session.query(FactSalesOrder).join(DimTerritory).filter(DimTerritory.name == territory_name_to_search).all()
for order in orders_from_territory:
    print(order.sales_order_number, order.sale_subtotal)

session.close()



# Executing the above code will print the names of all tables present in the SQLite database (sales_database.db).

from sqlalchemy import inspect

inspector = inspect(engine)

# Get table names
table_names = inspector.get_table_names()
for table_name in table_names:
    print(f"Table: {table_name}")
    columns = inspector.get_columns(table_name)
    for column in columns:
        print(f"  - {column['name']} ({column['type']})")
    print()
