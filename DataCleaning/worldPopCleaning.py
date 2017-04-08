import xml.etree.ElementTree as ET
import csv

tree = ET.parse('workingPop.xml')
root = tree.findall('data/record')

record = [['Country','Year','workingPop']]

for child in root:
	data = []
	for ele in child:
		try:
			t = ele.text.replace(u"\u2018", "'").replace(u"\u2019", "'")
		except:
			t=ele.text
		data.append(t)
	del data[1]
	record.append(data)

with open("workPop.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerows(record)
