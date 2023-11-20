import hashlib

def HashFunction(text):
  hex_hash_value = hashlib.sha256(text.encode()).hexdigest()
  bin_hash_value = "{0:08b}".format(int(hex_hash_value, 16))
  return bin_hash_value

def MD5EncodeFunction(text):
  result = hashlib.md5(str.encode('utf-8')).hexdigest()
  return result



def MD5DecodeFunction(text):
  for i in range(123):
    str = text.replace(hashlib.md5(chr(i).encode()).hexdigest(), chr(i))
  return str
