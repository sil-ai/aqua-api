# tests/utilities/test_verify_password.py

from security_routes.utilities import verify_password
import bcrypt

def test_verify_password():
    password = "test123"
    hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

    assert verify_password(password, hashed_password)
    assert not verify_password("wrongpassword", hashed_password)
