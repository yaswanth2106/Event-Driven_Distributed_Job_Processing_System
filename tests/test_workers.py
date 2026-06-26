import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import pytest
import struct
import hmac
import hashlib
import base64
from workers.job_handlers import audit_password, verify_totp, verify_signature

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except ImportError:
    ed25519 = None

def test_password_leak_audit():
    # 1. Test secure password
    res1 = audit_password("SuperSecureLongPassword2026!!")
    assert res1["leaked"] is False
    assert res1["strength_rating"] == "STRONG"
    assert len(res1["hash_prefix"]) == 5

    # 2. Test common leaked password
    res2 = audit_password("password")
    assert res2["leaked"] is True
    assert res2["leak_count"] == 2349812
    assert res2["strength_rating"] == "WEAK"

    # 3. Test short password
    res3 = audit_password("123")
    assert res3["leaked"] is True
    assert res3["strength_rating"] == "WEAK"

    # 4. Test empty password validation
    with pytest.raises(ValueError, match="Password is required"):
        audit_password("")

def test_mfa_totp_verifier():
    secret = "JBSWY3DPEHPK3PXP" 
    

    key = base64.b32decode(secret)
    curr_time = time.time()
    counter = int(curr_time / 30)
    
    msg = struct.pack(">Q", counter)
    hmac_hash = hmac.new(key, msg, hashlib.sha1).digest()
    offset = hmac_hash[-1] & 0x0f
    truncated = struct.unpack(">I", hmac_hash[offset:offset+4])[0] & 0x7fffffff
    expected_code = f"{(truncated % 1000000):06d}"

    # 1. Test valid code
    res1 = verify_totp(secret, expected_code)
    assert res1["valid"] is True
    assert res1["drift"] == 0

    # 2. Test valid code with drift 
    msg_drift = struct.pack(">Q", counter - 1)
    hmac_hash_drift = hmac.new(key, msg_drift, hashlib.sha1).digest()
    offset_drift = hmac_hash_drift[-1] & 0x0f
    truncated_drift = struct.unpack(">I", hmac_hash_drift[offset_drift:offset_drift+4])[0] & 0x7fffffff
    expected_code_drift = f"{(truncated_drift % 1000000):06d}"
    
    res2 = verify_totp(secret, expected_code_drift)
    assert res2["valid"] is True
    assert res2["drift"] == -1

    # 3. Test invalid code
    res3 = verify_totp(secret, "999999")
    assert res3["valid"] is False
    assert res3["drift"] is None

    # 4. Test invalid base32 secret
    with pytest.raises(ValueError, match="Invalid Base32 secret key"):
        verify_totp("INVALID_SECRET_CHARS_123!", "123456")

def test_signature_verifier():
    if ed25519 is None:
        pytest.skip("cryptography library is not installed")
        
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    message = "Authentic security payload verification"
    signature_bytes = private_key.sign(message.encode('utf-8'))
    
    pub_key_hex = public_key.public_bytes_raw().hex()
    signature_hex = signature_bytes.hex()
    
    # 1. Verify correct signature
    res1 = verify_signature(pub_key_hex, message, signature_hex)
    assert res1["valid"] is True
    assert res1["algorithm"] == "Ed25519"
    assert res1["error"] is None

    # 2. Verify bad message
    res2 = verify_signature(pub_key_hex, "Tampered message payload", signature_hex)
    assert res2["valid"] is False
    assert res2["error"] is not None

    # 3. Verify bad signature hex length validation
    with pytest.raises(ValueError, match="must be exactly 64 bytes"):
        verify_signature(pub_key_hex, message, "aabbcc")

    # 4. Verify bad public key length validation
    with pytest.raises(ValueError, match="must be exactly 32 bytes"):
        verify_signature("aabbcc", message, signature_hex)

if __name__ == "__main__":
    pytest.main(["-v", __file__])
