from datetime import datetime, timedelta
import os

filter_datetime = (datetime.today()-timedelta(days=50)).strftime('%Y-%m-%d 00:00:00')
print(filter_datetime)

# class Apple:
# 	manufacturer = 'Apple Inc.'
# 	contactWebsite = 'www.apple.com'
# 	def __init__(self, yearofManufacture, material):
# 		self.yearofManufacture = yearofManufacture
# 		self.material = material		

# 	def contactDetails(self):
# 		print(f'Contact on {self.contactWebsite}')

# class Macbook(Apple):
# 	def __init__(self, yearofManufacture, material, size, price):
# 		super().__init__(yearofManufacture, material)
# 		self.price = price
# 		self.size = size

# 	def manufactureDetail(self):
# 		print(f'Macbook is manufactured in {self.yearofManufacture} of {self.manufacturer}')
# 		print(self.size)
# 		print(self.price)

# 	def contactDetails(self):
# 		print(f'Please contact on {self.contactWebsite}')

# apple = Apple('2010', 'steel')
# apple.contactDetails()
# macbook = Macbook('2017', 'wood', '16/9', 500)
# macbook.manufactureDetail()
# macbook.contactDetails()