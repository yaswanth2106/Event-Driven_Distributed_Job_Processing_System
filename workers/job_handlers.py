import hashlib
import hmac
import struct
import time
import base64

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except ImportError:
    ed25519 = None

COMMON_PASSWORDS = {
    "password": 2349812,
    "123456": 15829302,
    "123456789": 8392019,
    "qwerty": 4920192,
    "admin": 3892012,
    "password123": 2819201,
    "welcome": 1920192,
    "letmein": 820192,
    "12345": 6392012
}

def audit_password(password):
    if not password:
        raise ValueError("Password is required")
    
    sha1_hash = hashlib.sha1(password.encode('utf-8')).hexdigest().upper()
    prefix = sha1_hash[:5]
    suffix = sha1_hash[5:]
    
    leaked = False
    count = 0
    
    if password.lower() in COMMON_PASSWORDS:
        leaked = True
        count = COMMON_PASSWORDS[password.lower()]
    elif len(password) < 8:
        leaked = True
        count = 120 
        
    return {
        "leaked": leaked,
        "hash_prefix": prefix,
        "leak_count": count,
        "password_length": len(password),
        "strength_rating": "WEAK" if leaked or len(password) < 8 else ("STRONG" if len(password) >= 12 else "MEDIUM"),
        "audit_timestamp": time.time()
    }

def verify_totp(secret, code):
    if not secret or not code:
        raise ValueError("Secret and code are required")
        
    secret = secret.replace(" ", "").upper()
    missing_padding = len(secret) % 8
    if missing_padding:
        secret += "=" * (8 - missing_padding)
        
    try:
        key = base64.b32decode(secret)
    except Exception as e:
        raise ValueError(f"Invalid Base32 secret key: {str(e)}")
        
    code = code.replace(" ", "")
    if not code.isdigit() or len(code) != 6:
        raise ValueError("Code must be a 6-digit number")
        
    curr_time = time.time()
    time_step = 30
    current_counter = int(curr_time / time_step)
    
    valid = False
    matched_drift = None
    
    for drift in [-1, 0, 1]:
        counter = current_counter + drift
        msg = struct.pack(">Q", counter)
        hmac_hash = hmac.new(key, msg, hashlib.sha1).digest()
        
        offset = hmac_hash[-1] & 0x0f
        truncated = struct.unpack(">I", hmac_hash[offset:offset+4])[0] & 0x7fffffff
        expected_code = truncated % 1000000
        
        if f"{expected_code:06d}" == code:
            valid = True
            matched_drift = drift
            break
            
    return {
        "valid": valid,
        "drift": matched_drift if valid else None,
        "timestamp": curr_time,
        "time_step": time_step
    }

def verify_signature(public_key_hex, message, signature_hex):
    if not public_key_hex or not message or not signature_hex:
        raise ValueError("Public key, message, and signature are required")
        
    try:
        pub_key_bytes = bytes.fromhex(public_key_hex.strip())
    except Exception:
        raise ValueError("Public key must be a valid hex string")
        
    try:
        sig_bytes = bytes.fromhex(signature_hex.strip())
    except Exception:
        raise ValueError("Signature must be a valid hex string")
        
    if len(pub_key_bytes) != 32:
        raise ValueError(f"Ed25519 public key must be exactly 32 bytes (64 hex characters), got {len(pub_key_bytes)} bytes")
        
    if len(sig_bytes) != 64:
        raise ValueError(f"Ed25519 signature must be exactly 64 bytes (128 hex characters), got {len(sig_bytes)} bytes")
        
    if ed25519 is None:
        raise RuntimeError("cryptography library is not available on this worker")
        
    try:
        public_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_key_bytes)
        msg_bytes = message.encode('utf-8')
        public_key.verify(sig_bytes, msg_bytes)
        valid = True
        err = None
    except Exception as e:
        valid = False
        err = str(e) or "Signature verification failed"
        
    return {
        "valid": valid,
        "algorithm": "Ed25519",
        "error": err,
        "timestamp": time.time()
    }
