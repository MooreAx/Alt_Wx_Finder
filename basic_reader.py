import 

weather_file = "data/CYTH_20250901-20251001.txt"

with open(weather_file) as file_object:
    contents = file_object.read()


print(contents)

metars = {}

metars