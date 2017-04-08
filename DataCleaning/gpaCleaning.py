import xml.etree.ElementTree as ET
import csv

tree = ET.parse('gpa.xml')
root = tree.findall('data/record')

record = [['Country','Year','GDP','Note']]

for child in root:
	data = []
	for ele in child:
		t = ele.text
		data.append(t)
	record.append(data)

with open("gpaCleaned.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(record)