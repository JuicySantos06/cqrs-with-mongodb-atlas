from re import S
from time import sleep
from pymongo import MongoClient
from faker import Faker
from datetime import datetime, date
from faker_credit_score import CreditScore
import random
import radar
import financialStatementParams

fake = Faker()
fake.add_provider(CreditScore)

def getMongoDatabase(): 
    mongoClient = MongoClient(financialStatementParams.CONNECTION_STRING)
    return mongoClient[financialStatementParams.DATABASE_NAME]

def getDate():
    return radar.random_datetime(start='1961-01-01', stop='2000-12-31T23:59:59')

def getFirstName():
    return fake.first_name()

def getLastName():
    return fake.first_name()

def getIban():
    return fake.iban()

def getGender():
    return random.choice(["Male", "Female"])

def getMaritalStatus():
    return random.choice(["Single", "Married", "Widowed", "Separated"])

def getPhoneNumber():
    return fake.phone_number()

def getStreetNumber():
    return fake.building_number()

def getProfession():
    return fake.job()

def getCompany():
    return fake.company()

def getCreditScore():
    return fake.credit_score()

def getCreditScoreProvider():
    return fake.credit_score_provider()

def getCreditCardProvider():
    return fake.credit_card_provider()

def generateRandomNumberList(a,b):
    return random.sample(list(range(a, b + financialStatementParams.NUM_RECORDS)), financialStatementParams.NUM_RECORDS)  

def insert_customers_data():
    databaseObject = getMongoDatabase()
    collectionObject = databaseObject[financialStatementParams.COLLECTION_NAME]
    collectionObject.drop()
    
    salary_set = generateRandomNumberList(financialStatementParams.SALARY_START, financialStatementParams.SALARY_END)
    real_estate_income = generateRandomNumberList(financialStatementParams.REAL_ESTATE_INCOME_START, financialStatementParams.REAL_ESTATE_INCOME_END)
    savings_account = generateRandomNumberList(financialStatementParams.SAVINGS_ACCOUNT_START, financialStatementParams.SAVINGS_ACCOUNT_END)
    retirement_account = generateRandomNumberList(financialStatementParams.RETIREMENT_ACCOUNT_START, financialStatementParams.RETIREMENT_ACCOUNT_END)
    life_insurance = generateRandomNumberList(financialStatementParams.LIFE_INSURANCE_START, financialStatementParams.LIFE_INSURANCE_END)
    stocks_value = generateRandomNumberList(financialStatementParams.STOCKS_VALUE_START, financialStatementParams.STOCKS_VALUE_END)
    automobiles = generateRandomNumberList(financialStatementParams.AUTOMOBILES_START, financialStatementParams.AUTOMOBILES_END)
    accounts_payable = generateRandomNumberList(financialStatementParams.ACCOUNTS_PAYABLE_START, financialStatementParams.ACCOUNTS_PAYABLE_END)
    loans_life_insurance = generateRandomNumberList(financialStatementParams.LOANS_AGAINST_LIFE_INSURANCE_START, financialStatementParams.LOANS_AGAINST_LIFE_INSURANCE_END)
    mortgage_real_estate = generateRandomNumberList(financialStatementParams.MORTGAGE_ON_REAL_ESTATE_START, financialStatementParams.MORTGAGE_ON_REAL_ESTATE_END)
    loans_life_insurance = generateRandomNumberList(financialStatementParams.LOANS_AGAINST_LIFE_INSURANCE_START, financialStatementParams.LOANS_AGAINST_LIFE_INSURANCE_END)
    unpaid_taxes = generateRandomNumberList(financialStatementParams.UNPAID_TAXES_START, financialStatementParams.UNPAID_TAXES_END)
    legal_claims_judgements = generateRandomNumberList(financialStatementParams.LEGAL_CLAIMS_AND_JUDGMENTS_START, financialStatementParams.LEGAL_CLAIMS_AND_JUDGMENTS_END)
    provision_federal_income_tax_income = generateRandomNumberList(financialStatementParams.PROVISION_FOR_FEDERAL_INCOME_TAX_START, financialStatementParams.PROVISION_FOR_FEDERAL_INCOME_TAX_END)
    
    p = 0
    for idx in range(financialStatementParams.NUM_RECORDS):

        newCustomer = {
            "c_firstName": getFirstName(),
            "c_lastName" : getLastName(),
            "c_iban" : getIban(),
            "c_gender" : random.choice(["Male", "Female"]),
            "c_maritalStatus" : getMaritalStatus(),
            "c_birthdayDate" : getDate(),
            "c_homePhone" : getPhoneNumber(),
            "c_city" : fake.city(),
            "c_street" : fake.street_name(),
            "c_addressNumber" : getStreetNumber(),
            "c_profession" : getProfession(),
            "c_comppany": getCompany(),
            "c_creditScore": getCreditScore(),
            "c_creditScoreProvider": getCreditScoreProvider(),
            "c_creditCard": getCreditCardProvider(),
            "c_assets": {
                "c_salary" : salary_set.pop(p),
                "c_realEstateIncome" : real_estate_income.pop(p),
                "c_savingsAccount" : savings_account.pop(p),
                "c_retirementAccount" : retirement_account.pop(p),
                "c_lifeInsurance" : life_insurance.pop(p),
                "c_stocksValue" : stocks_value.pop(p),
                "c_automobiles" : automobiles.pop(p),
            },
            "c_liabilities": {
                "c_accountsPayable" : accounts_payable.pop(p),
                "c_loansAgainstLifeInsurance" : loans_life_insurance.pop(p),
                "c_mortgageOnRealEstate" : mortgage_real_estate.pop(p),
                "c_unpaidTaxes" : unpaid_taxes.pop(p),
                "c_legalClaimsAndJudgments" : legal_claims_judgements.pop(p),
                "c_provisionForFederalIncomeTax" : provision_federal_income_tax_income.pop(p)
            }
        }
        print("Inserting customer " + str(idx) + " financial statement data")
        collectionObject.insert_one(newCustomer)

if __name__ == '__main__':
    insert_customers_data()


    