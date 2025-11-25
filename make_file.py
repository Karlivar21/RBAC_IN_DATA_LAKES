import pandas as pd
import random, string

data = {
    "ID": list(range(1, 11)),
    "Name": ["Alice","Bob","Charlie","David","Eva","Frank","Grace","Hannah","Ian","Julia"],
    "Email": [
        "alice@example.com","bob@example.com","charlie@example.com","david@example.com","eva@example.com",
        "frank@example.com","grace@example.com","hannah@example.com","ian@example.com","julia@example.com"
    ],
    "Department": ["HR","Engineering","Marketing","Finance","Engineering","HR","Sales","Finance","Marketing","Sales"],
    "EncryptedSalary": [
        "gAAAAABpHxvqr_I-oNztk3kxAsXE4nJ6NYuGXo6I0J6E6-...",
        "gAAAAABpHxvqU9eVA3qgZLmRajlm7ofxzCEJ2lPlxOn0aE...",
        "gAAAAABpHxvqVql6Rbx2l09TSjGvJQAapJfaOVEheyhNcj...",
        "gAAAAABpHxvqi4qSi3cbxoyN6XT00TTPJw9P97EN6ylpB8...",
        "gAAAAABpHxvqQhOxZRr_h6TlLLWv0VMwj9yHigXPxCyLbV...",
        "gAAAAABpHxvqW6rlHk5OkFA81cOJwigNl_Th0_okxhBSJE...",
        "gAAAAABpHxvqDcZadUO9yaTDowpZrgbtZ5jXwzj47jwINR...",
        "gAAAAABpHxvq79CseHM3XGsm5eDn0NxGEL395ibQm-rkfs...",
        "gAAAAABpHxvqVXSKaBckB5Yk3BkKexwgxVE5WOIk4s6jxV...",
        "gAAAAABpHxvqWJKkKEK0M0rLUi9qM2noktanNNLoklrXV6..."
    ]
}

def random_password(length=14):
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(random.choice(chars) for _ in range(length))

data["Password"] = [random_password() for _ in range(10)]

df = pd.DataFrame(data)

df.to_parquet("employees.parquet", index=False)
print("Saved employees.parquet")
