import uuid


def gen_uid():
    uid = str(uuid.uuid4())
    print(uid)


if __name__ == "__main__":
    gen_uid()
