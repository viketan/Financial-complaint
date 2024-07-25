import os
import argparse
import sys
from src.exception import CustomException
from src.pipeline import TrainingPipeline 
from src.logger import logger
from src.config.pipeline.training import FinanceConfig


def start_training(start=False):
    try:
        if not start:
            return None
        print("Training Running")
        TrainingPipeline(FinanceConfig()).start()
    except Exception as e:
        raise CustomException(e, sys)


def main(training_status):
    try:
        start_training(start=training_status)
    except Exception as e:
        raise CustomException(e, sys)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-t", default=0, type=int, help="If provided 1, training will be done; else not")

        args = parser.parse_args()

        training_status = bool(args.t)

        main(training_status=training_status)
    except Exception as e:
        print(e)
        logger.exception(CustomException(e, sys))
