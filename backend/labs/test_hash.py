from src.core.hashing import calculate_content_hash

data1 = {"id": "101", "name": "Alice", "email": "alice@test.com"}
data2 = {"name": "Alice", "email": "alice@test.com", "id": "101"} # Ordre différent

hash1 = calculate_content_hash(data1)
hash2 = calculate_content_hash(data2)

print(f"Hash 1: {hash1}")
print(f"Hash 2: {hash2}")
print(f"✅ Identiques ? {hash1 == hash2}")