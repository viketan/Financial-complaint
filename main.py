import sys
from src.exception import CustomException
from src.logger import logger

def main():
    try:
        logger.info("Testing logging and exception")
        result =1/0
    except Exception as e:
        logger.error(f"Error occured in main: {e}")
        raise CustomException(e,sys)
    

if __name__=="__main__":
    main()