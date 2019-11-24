import string

try:
    import secrets
except ImportError:
    import random as secrets


def gen_password(length=20, alphabet=None):
    if not alphabet:
        alphabet = string.ascii_letters + string.digits

    password = "".join([secrets.choice(alphabet) for _ in range(length)])
    print(password)


if __name__ == "__main__":
    gen_password(length=80)
