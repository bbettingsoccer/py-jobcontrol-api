import random
import string


class GeneralFunctions:
    @staticmethod
    def random_number_characters():
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(5))


