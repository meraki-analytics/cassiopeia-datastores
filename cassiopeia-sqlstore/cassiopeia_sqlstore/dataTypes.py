switcher = {
    "platform": "String(5)",
    "rotationKeyId": "Integer",
}


def getType(string):
    return switcher.get(string)
