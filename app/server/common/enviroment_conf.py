import os
from dotenv import load_dotenv, dotenv_values, find_dotenv


def env_check():
    env_file = None

    if os.environ['ENVIRONMENT_TYPE'] == 'DEV':
        print("ENV DEV")
        env_file = find_dotenv("./env/dev.env")
        load_dotenv(env_file)

    elif os.environ['ENVIRONMENT_TYPE'] == 'PRO':
        env_file = find_dotenv("./env/pro.env")
        load_dotenv(env_file)
    load_dotenv(env_file)
