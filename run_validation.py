from bees_breweries.validations.data_validation import DataLakeValidator

def main():
    validator = DataLakeValidator()
    validator.run()

if __name__ == "__main__":
    main()
