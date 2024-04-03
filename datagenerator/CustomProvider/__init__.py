from faker.providers import BaseProvider
import random
from faker import Faker

class ClothingProductProvider(BaseProvider):


    def genProductName(self):
        return random.choices(self.AdsMediaTypes, weights=[10, 10, 10, 10, 10, 50], k=1)[0]

class PromotionProvider(BaseProvider):
    def adsMediaTypes(self):
        return random.choice(['TV', 'Video', 'Influencer Marketing', 'Billboard', 'Social Media', 'Online', 'Newspaper'])
    
    def promotionType(self):
        return random.choice(['Percent Off', 'Fixed Amount Off','Buy One Get One', 'Bundle Discounts' ,'Free Shipping', 'Free Gift with Purchase' , 'Loyalty Points', 'Gift Card', 'Coupon'])

class EmployeeProvider(BaseProvider):
    jobTitles = ['Sales Associate', 'Cashier', 'Stock Clerk','Assistant Manager']

    def jobTitle(self):
        return random.choices(self.jobTitles, weights=[50, 10, 10, 5], k=1)[0]
    
if __name__ == "__main__":
    fake = Faker()
    fake.add_provider(EmployeeProvider)
    print(fake.jobTitle())