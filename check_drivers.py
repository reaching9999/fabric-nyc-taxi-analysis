import pyodbc

print("--- Installed ODBC Drivers ---")
drivers = pyodbc.drivers()
if not drivers:
    print("No drivers found!")
else:
    for driver in drivers:
        print(f" - {driver}")
print("----------------------------")
